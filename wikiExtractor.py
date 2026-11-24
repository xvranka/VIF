# run: SPARK_DRIVER_MEMORY=12g python wikiExtractor.py data/wiki/enwiki-latest-pages-articles.xml.bz2

# Wikipedia extractor
#
# Reads the whole English Wikipedia dump (26 GB) with Spark and keeps only what we need.
# Spark splits the dump so all cores work at once (about 12 minutes).
#
# We keep two kinds of pages and drop the rest:
#
# 1. Fossil animal pages (24 999), saved to extracted-spark/wiki/pages
#    A page counts if its info box (taxobox) has a fossil_range.
#    From each page we take:
#      - the title and the scientific name
#      - when it lived, as text and in millions of years
#      - who named it and in which year
#      - the type species
#      - the rock formations where it was found
#      - the wiki categories
#
# 2. Redirects (12 million), saved to extracted-spark/wiki/redirects
#    A redirect is an old or other name that sends you to the real page,
#    e.g. "Dracorex" -> "Pachycephalosaurus". The joiner uses them to find more matches.
#
# without the dump argument it reads the small 300 MB part, for testing

import argparse
import logging
import re
from html import unescape

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

from sparkUtils import createSpark

log = logging.getLogger(__name__)

DUMP_FILE = "data/wiki/enwiki-latest-pages-articles1.xml-p1p41242.bz2"
PAGES_OUTPUT = "extracted-spark/wiki/pages"
REDIRECTS_OUTPUT = "extracted-spark/wiki/redirects"

# the dump is one xml file, hadoop splits it into records at every page
PAGE_DELIMITER = "</page>"

# xml page
# <title>Allosaurus</title>
TITLE_REGEX = re.compile(r"<title>(.*?)</title>")
# namespace, 0 = article, other numbers are talk, user, template, category ... pages
NS_REGEX = re.compile(r"<ns>(\d+)</ns>")
# <redirect title="Pachycephalosaurus" /> only on redirect pages
REDIRECT_REGEX = re.compile(r'<redirect title="([^"]*)"')
# the article itself, written in wikitext
TEXT_REGEX = re.compile(r"<text[^>]*>(.*?)</text>", re.S)

# wikitext
# "| key = value" lines of the taxobox ([ \t] so an empty value does not continue on the next line)
INFOBOX_PARAM_REGEX = re.compile(r"^[ \t]*\|[ \t]*(\w+)[ \t]*=[ \t]*(.*?)[ \t]*$", re.M)
# taxobox lines we read, the rest (image, caption, synonyms, ...) is ignored
TAXOBOX_PARAMS = ["fossil_range", "taxon", "genus", "species", "authority", "type_species"]
# {{Geological range|155|143.1}} / {{fossil range|155|143.1}} holds the range in millions of years
RANGE_MA_REGEX = re.compile(r"\{\{\s*(?:Geological range|fossil ?range)\s*\|\s*(\d+(?:\.\d+)?)\s*(?:\|\s*(\d+(?:\.\d+)?))?", re.I)
# a year between 1700 and 2029, "Marsh, 1877" -> 1877
YEAR_REGEX = re.compile(r"\b(1[789]\d\d|20[0-2]\d)\b")
# [[Category:Dinosaurs of the Morrison Formation]] -> Dinosaurs of the Morrison Formation
CATEGORY_REGEX = re.compile(r"\[\[\s*Category\s*:\s*([^\]|]+)")
# links to rock formations: [[Morrison Formation]] or [[Hell Creek Formation|Hell Creek]]
# (no ":" in the target, so File:/Category: links are skipped)
FORMATION_REGEX = re.compile(r"\[\[([^\]|#:]*\bFormation)\b[^\]]*\]\]")

# cleaning of taxobox values
# <ref>...</ref> citations
REF_REGEX = re.compile(r"<ref[^>]*/>|<ref[^>]*>.*?</ref>", re.S)
# innermost {{...}} template, removed repeatedly because templates can be nested
INNER_TEMPLATE_REGEX = re.compile(r"\{\{[^{}]*\}\}")
# [[target|label]] -> label, [[target]] -> target
LINK_REGEX = re.compile(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]")
# html tags (<small>, <br/>) and ''italic'' / '''bold''' quotes
MARKUP_REGEX = re.compile(r"<[^>]+>|'{2,}")

# columns of a fossil taxon page, missing values are null, numbers are typed so the joiner can compare them
PAGE_SCHEMA = StructType([
    StructField("wiki_title", StringType()),
    StructField("taxon", StringType()),
    StructField("fossil_range", StringType()),
    StructField("range_start_ma", DoubleType()),
    StructField("range_end_ma", DoubleType()),
    StructField("authority", StringType()),
    StructField("authority_year", IntegerType()),
    StructField("type_species", StringType()),
    StructField("formations", ArrayType(StringType())),
    StructField("categories", ArrayType(StringType())),
])
# columns of a redirect: old name -> page it points to
REDIRECT_SCHEMA = StructType([StructField("redirect_title", StringType()), StructField("wiki_title", StringType())])

# taxobox value -> plain text, keeps the readable text of links (None if nothing is left)
# "Marsh, [[1877 in paleontology|1877]]<ref>...</ref>" -> "Marsh, 1877"
def cleanWikitext(text):
    text = REF_REGEX.sub("", text)
    previous = None
    while previous != text:
        previous = text
        text = INNER_TEMPLATE_REGEX.sub("", text)
    text = LINK_REGEX.sub(r"\1", text)
    text = MARKUP_REGEX.sub("", text)
    return " ".join(text.split()).strip(" ,;~") or None

# returns the taxobox values we care about as {key: raw wikitext value}
def taxoboxParams(text):
    params = {}
    for key, value in INFOBOX_PARAM_REGEX.findall(text):
        if key in TAXOBOX_PARAMS and key not in params and value:
            params[key] = value
    return params

# removes duplicates and empty values, keeps the order
def distinct(values):
    return list(dict.fromkeys(v.strip() for v in values if v.strip()))

# one fossil taxon page -> row in PAGE_SCHEMA order
def parsePage(title, text, params):
    rangeMa = RANGE_MA_REGEX.search(params["fossil_range"])
    authority = cleanWikitext(params.get("authority", ""))
    year = YEAR_REGEX.search(authority or "")
    # Automatic taxobox has "taxon", Speciesbox has "genus" + "species"
    taxon = params.get("taxon") or " ".join(params[k] for k in ("genus", "species") if k in params)
    return (
        title,
        cleanWikitext(taxon),
        cleanWikitext(params["fossil_range"]),
        float(rangeMa.group(1)) if rangeMa else None,
        float(rangeMa.group(2)) if rangeMa and rangeMa.group(2) else None,
        authority,
        int(year.group(1)) if year else None,
        cleanWikitext(params.get("type_species", "")),
        distinct(FORMATION_REGEX.findall(text)),
        distinct(CATEGORY_REGEX.findall(text)),
    )

# decides what happens with one page of the dump, runs on the spark workers
# returns ("page", row) for a fossil taxon article, ("redirect", row) for an article redirect, None otherwise (dropped)
def parseRecord(record):
    title = TITLE_REGEX.search(record)
    ns = NS_REGEX.search(record)
    if not title or not ns or ns.group(1) != "0":
        return None
    title = unescape(title.group(1))
    redirect = REDIRECT_REGEX.search(record)
    if redirect:
        return ("redirect", (title, unescape(redirect.group(1))))
    text = TEXT_REGEX.search(record)
    # cheap string test first, most pages do not mention fossil_range at all
    if not text or "fossil_range" not in text.group(1):
        return None
    text = unescape(text.group(1))  # the wikitext is xml escaped in the dump
    # a page about a fossil taxon has a taxobox with a filled in fossil_range
    params = taxoboxParams(text)
    if "fossil_range" not in params:
        return None
    return ("page", parsePage(title, text, params))

# main function to run everything
# reads the dump split at </page>, every core parses its own part, keeps fossil taxon pages and redirects, writes both
def extractAll(spark, dump=DUMP_FILE, pagesOutput=PAGES_OUTPUT, redirectsOutput=REDIRECTS_OUTPUT):
    # hadoop text reader with </page> instead of newline as the record end, pair = (position in file, page text)
    records = spark.sparkContext.newAPIHadoopFile(
        dump,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": PAGE_DELIMITER},
    ).map(lambda pair: pair[1])
    # cache: the result is used twice (pages, redirects), without it the whole dump would be read twice
    parsed = records.map(parseRecord).filter(lambda result: result is not None).cache()

    # fossil taxon pages as json (readable), the same title saved twice is written once
    pages = spark.createDataFrame(parsed.filter(lambda r: r[0] == "page").map(lambda r: r[1]), PAGE_SCHEMA)
    pages.dropDuplicates(["wiki_title"]).write.mode("overwrite").json(pagesOutput)
    # redirects as parquet (compressed, columnar), 12 million rows are much smaller and faster to join than json
    redirects = spark.createDataFrame(parsed.filter(lambda r: r[0] == "redirect").map(lambda r: r[1]), REDIRECT_SCHEMA)
    redirects.write.mode("overwrite").parquet(redirectsOutput)

    # counts are read back from the written files, counting the dataframes would parse the dump again
    log.info("wrote %d fossil taxon pages to %s", spark.read.json(pagesOutput).count(), pagesOutput)
    log.info("wrote %d redirects to %s", spark.read.parquet(redirectsOutput).count(), redirectsOutput)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="extract fossil taxon pages and redirects from a wikipedia dump")
    parser.add_argument("dump", nargs="?", default=DUMP_FILE, help="path to a pages-articles .xml.bz2 dump (or a glob)")
    args = parser.parse_args()
    spark = createSpark("wiki-extractor")
    extractAll(spark, args.dump)
    spark.stop()
