from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("TurnoverStreamingData").getOrCreate()
ssc = StreamingContext(spark, batchDuration=2) 

topic = "turnover"
consumerGroup = "spark_consumer_group"

kafkaStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("group.id", consumerGroup) \
    .load()

errorMessages = kafkaStream.filter(lambda message: "left" in message.value)

errorMessages.print()

ssc.start()
ssc.awaitTermination()