from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkExampleApp") \
    .config("spark.memory.fraction", 0.6) \
    .config("spark.storage.fraction", 0.5) \
    .getOrCreate()

spark.stop()