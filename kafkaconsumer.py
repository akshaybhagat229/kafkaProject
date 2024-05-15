from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
#pip install kafka-python

df = spark \
 .readStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "localhost:9092") \
 .option("subscribe", "hello") \
 .load()
df.printSchema()
ndf=df.selectExpr("CAST(value AS STRING)")

log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

logs_df = ndf.select(regexp_extract('value', log_reg, 1).alias('ip'),
                         regexp_extract('value', log_reg, 4).alias('date'),
                         regexp_extract('value', log_reg, 6).alias('request'),
                         regexp_extract('value', log_reg, 10).alias('referrer'))

myconf = {
        "sfURL": "kvwjswf-eb71499.snowflakecomputing.com/",
        "sfUser": "AKSHAYBHAGAT229",
        "sfPassword": "Akshay@7021",
        "sfDatabase": "NEW",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH"
    }


def foreach_batch_function(df, epoch_id):
   df1 = df.withColumn("ts", current_timestamp())
   df1.write.mode("append").format("net.snowflake.spark.snowflake").options(**myconf).option("dbtable", "log2024").save()
   # Transform and write batchDF
   pass


logs_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()



#logs_df.writeStream.outputMode("append").format("console").start().awaitTermination()

#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()
