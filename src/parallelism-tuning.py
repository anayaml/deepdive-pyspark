from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data = spark.sparkContext.textFile("data.txt", minPartitions=4)

word_counts = data.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

word_counts.collect().foreach(print)

spark.stop()