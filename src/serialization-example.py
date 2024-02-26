from pyspark.sql import SparkSession

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

conf = SparkConf()
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark = SparkSession.builder.appName("SparkSerialization").config(conf=conf).getOrCreate()

people = [Person("Ana", 30), Person("João", 25)]

people_rdd = spark.sparkContext.parallelize(people)

people_rdd.foreach(print)

spark.stop()