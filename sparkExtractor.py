# run: python sparkExtractor.py

import logging

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from sparkUtils import createSpark

log = logging.getLogger(__name__)

SPECIES_DIR = "data/prehistoric-wildlife/species/*.html"
ARTICLES_DIR = "data/prehistoric-wildlife/articles/*.html*"
SPECIES_OUTPUT = "extracted-spark/species"
ARTICLES_OUTPUT = "extracted-spark/articles"

# spark schema definitions
# fact table labels found on the species pages, every record gets all of them (missing = null)
FACT_FIELDS = [
    "name", "phonetic", "named_by", "classification", "diet", "species",
    "size", "known_locations", "time_period", "fossil_representation",
]
SPECIES_SCHEMA = StructType(
    [StructField("url", StringType()), StructField("title", StringType())]
    + [StructField(f, StringType()) for f in FACT_FIELDS]
    + [StructField("in_depth", StringType()), StructField("further_reading", ArrayType(StringType()))]
)
ARTICLES_SCHEMA = StructType([
    StructField("url", StringType()),
    StructField("title", StringType()),
    StructField("in_depth", StringType()),
    StructField("further_reading", ArrayType(StringType())),
])

# runs on the workers: the same regex extraction as extractorSpecies.py, record as a tuple in schema order
def speciesRow(html):
    from extractorSpecies import extractPage
    record = extractPage(html)
    if record is None or record["url"] is None:
        return None
    return tuple(record.get(field.name) for field in SPECIES_SCHEMA.fields)

def articleRow(html):
    from extractorArticles import extractPage
    record = extractPage(html)
    if record["url"] is None:
        return None
    return tuple(record[field.name] for field in ARTICLES_SCHEMA.fields)

# reads all html files of a directory as (path, html), extracts records in parallel,
# drops pages without data and the same url saved twice, writes json lines
def extract(spark, pattern, rowFunction, schema, output):
    pages = spark.sparkContext.wholeTextFiles(pattern, minPartitions=32)
    rows = pages.map(lambda page: rowFunction(page[1])).filter(lambda row: row is not None)
    df = spark.createDataFrame(rows, schema).dropDuplicates(["url"])
    df.write.mode("overwrite").json(output)
    count = spark.read.json(output).count()
    log.info("wrote %d records to %s", count, output)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    spark = createSpark("prehistoric-extractor", pyFiles=["extractorSpecies.py", "extractorArticles.py"])
    extract(spark, SPECIES_DIR, speciesRow, SPECIES_SCHEMA, SPECIES_OUTPUT)
    extract(spark, ARTICLES_DIR, articleRow, ARTICLES_SCHEMA, ARTICLES_OUTPUT)
    spark.stop()
