from pyspark.sql import SparkSession

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

spark = SparkSession.builder.appName("SparkSerialization").getOrCreate()

people = [Person("Ana", 30), Person("John", 25)]

people_rdd = spark.sparkContext.parallelize(people)

people_rdd.foreach(print)

spark.stop()