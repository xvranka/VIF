# run: SPARK_DRIVER_MEMORY=12g python joiner.py

import json
import logging
import re
import unicodedata
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from sparkUtils import createSpark

log = logging.getLogger(__name__)

SPECIES_INPUT = "extracted-spark/species"
WIKI_PAGES_INPUT = "extracted-spark/wiki/pages"
WIKI_REDIRECTS_INPUT = "extracted-spark/wiki/redirects"
JOINED_OUTPUT = "extracted-spark/joined"
STATS_OUTPUT = "extracted-spark/joinStats.json"

# our titles carry aliases: "Megalonyx a.k.a. Jefferson's ground sloth", "Mammuthus columbi (Columbian mammoth)",
# "Dinornis Including D. novaezealandiae, ..." -> the scientific name is the part before the alias
TITLE_ALIAS_REGEX = re.compile(r"\s+(?:a\.?k\.?a\.?|aka|including|also known as)\b|[(,]", re.I)
# wiki titles carry a disambiguation suffix: "Ajax (dinosaur)" -> "Ajax", a redirect may point to a section
WIKI_SUFFIX_REGEX = re.compile(r"\s*\([^)]*\)\s*$|#.*$")
# species field "A. celestae", "T. rex, T. imperator" -> epithets celestae, rex, imperator
EPITHET_REGEX = re.compile(r"\b[A-Z][a-z]*\.\s*([a-z]{3,})\b")

# join key: no diacritics, lowercase, only letters and digits separated by single spaces
def normalize(name):
    if not name:
        return None
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    return name or None

def titleKey(title):
    return normalize(TITLE_ALIAS_REGEX.split(title or "", maxsplit=1)[0])

def wikiKey(title):
    return normalize(WIKI_SUFFIX_REGEX.sub("", title or ""))

# genus from our title + every epithet from the species field -> binomial keys, e.g. "aardonyx celestae"
def binomialKeys(title, species):
    key = titleKey(title)
    if not key or not species:
        return []
    genus = key.split()[0]
    return list(dict.fromkeys(f"{genus} {epithet}" for epithet in EPITHET_REGEX.findall(species)))

titleKeyUdf = F.udf(titleKey, StringType())
wikiKeyUdf = F.udf(wikiKey, StringType())
binomialKeysUdf = F.udf(binomialKeys, ArrayType(StringType()))

# keys under which a wiki page can be found, the rank says how reliable the key is (lower = better)
def wikiKeys(pages, redirects):
    byTitle = pages.select(wikiKeyUdf("wiki_title").alias("key"), "wiki_title", F.lit(0).alias("wiki_rank"), F.lit("title").alias("wiki_via"))
    byTaxon = pages.select(wikiKeyUdf("taxon").alias("key"), "wiki_title", F.lit(0).alias("wiki_rank"), F.lit("taxon").alias("wiki_via"))
    # redirects (old names, synonyms, binomials) that point to one of the fossil taxon pages
    targets = redirects.withColumn("target", F.regexp_replace("wiki_title", "#.*$", "")).drop("wiki_title")
    byRedirect = targets.join(pages.select("wiki_title"), targets.target == pages.wiki_title).select(
        wikiKeyUdf("redirect_title").alias("key"), "wiki_title", F.lit(1).alias("wiki_rank"), F.lit("redirect").alias("wiki_via"))
    byTypeSpecies = pages.select(wikiKeyUdf("type_species").alias("key"), "wiki_title", F.lit(2).alias("wiki_rank"), F.lit("type_species").alias("wiki_via"))
    return byTitle.union(byTaxon).union(byRedirect).union(byTypeSpecies).where(F.col("key").isNotNull()).dropDuplicates(["key", "wiki_title"])

# keys of our species: the cleaned title, then the binomials from the species field
def speciesKeys(species):
    byTitle = species.select("url", titleKeyUdf("title").alias("key"), F.lit(0).alias("species_rank"), F.lit("title").alias("species_via"))
    byBinomial = species.select("url", F.explode(binomialKeysUdf("title", "species")).alias("key"),
                                F.lit(3).alias("species_rank"), F.lit("binomial").alias("species_via"))
    return byTitle.union(byBinomial).where(F.col("key").isNotNull())

# every species gets its best wiki page: lowest combined rank, ties broken by the wiki title
def bestMatches(species, pages, redirects):
    candidates = speciesKeys(species).join(wikiKeys(pages, redirects), "key").withColumn(
        "rank", F.col("species_rank") + F.col("wiki_rank"))
    best = Window.partitionBy("url").orderBy("rank", "wiki_title")
    return (candidates.withColumn("n", F.row_number().over(best)).where("n = 1")
            .select("url", "wiki_title", F.concat_ws("->", "species_via", "wiki_via").alias("match_type")))

# numbers for the report
def statistics(species, joined):
    total = species.count()
    matched = joined.where(F.col("wiki_title").isNotNull()).cache()
    matchedCount = matched.count()
    # does the year in our "named_by" agree with the wiki taxobox authority year
    withYears = matched.withColumn("our_year", F.regexp_extract("named_by", r"\b(1[789]\d\d|20[0-2]\d)\b", 1)) \
        .where((F.col("our_year") != "") & F.col("authority_year").isNotNull())
    return {
        "species_total": total,
        "species_matched": matchedCount,
        "species_matched_percent": round(100 * matchedCount / total, 2) if total else 0,
        "unique_wiki_pages": matched.select("wiki_title").distinct().count(),
        "match_types": {r["match_type"]: r["count"] for r in matched.groupBy("match_type").count().collect()},
        "matched_with_range_ma": matched.where(F.col("range_start_ma").isNotNull()).count(),
        "matched_with_formations": matched.where(F.size("formations") > 0).count(),
        "named_year_compared": withYears.count(),
        "named_year_agrees": withYears.where(F.col("our_year").cast("int") == F.col("authority_year")).count(),
        "wiki_pages_shared_by_several_species": matched.groupBy("wiki_title").count().where("count > 1").count(),
        "unmatched_sample": [r["title"] for r in joined.where(F.col("wiki_title").isNull()).select("title").limit(20).collect()],
    }

# left join: all our species, wiki columns are null where no page was found
def joinAll(spark):
    species = spark.read.json(SPECIES_INPUT)
    pages = spark.read.json(WIKI_PAGES_INPUT)
    redirects = spark.read.parquet(WIKI_REDIRECTS_INPUT)
    matches = bestMatches(species, pages, redirects)
    joined = species.join(matches, "url", "left").join(pages, "wiki_title", "left")
    joined.write.mode("overwrite").json(JOINED_OUTPUT)

    stats = statistics(species, spark.read.json(JOINED_OUTPUT))
    Path(STATS_OUTPUT).write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("join statistics written to %s:\n%s", STATS_OUTPUT, json.dumps(stats, ensure_ascii=False, indent=2))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    spark = createSpark("species-wiki-join")
    joinAll(spark)
    spark.stop()
