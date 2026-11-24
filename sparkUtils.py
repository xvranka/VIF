import os
import sys

from pyspark.sql import SparkSession

SPARK_JAVA_HOME = "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
if os.path.isdir(SPARK_JAVA_HOME):
    os.environ["JAVA_HOME"] = SPARK_JAVA_HOME

# workers must use the same python as the driver (the venv)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# local spark session using all cores, modules in pyFiles are shipped to the workers
def createSpark(name, pyFiles=()):
    spark = (
        SparkSession.builder.appName(name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    for path in pyFiles:
        spark.sparkContext.addPyFile(path)
    return spark
