from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#pip install kafka-python

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "Naruto") \
  .load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")
df1=ndf.withColumn("Name",split("value",',')[0])\
        .withColumn("Age",split("value",',')[1])\
        .withColumn("City",split("value",',')[2]).drop("value")
df1.writeStream.outputMode("append").format("console").start().awaitTermination()