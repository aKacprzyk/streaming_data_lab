from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
 
batch_counter = {"count": 0}
 
def process_batch(df, batch_id):
    batch_counter["count"] += 1
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)
    if batch_counter["count"] % 7 == 0:
        spark.stop()
 
spark = SparkSession.builder.appName("RealTimeEcommerce").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
 
# StringType(), TimestampType(), DoubleType()
 
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType())
])
 
stream = (spark.readStream
          .schema(schema)
          .json("data/stream"))
 
 agg1 = (stream.groupBy("event_type").count())

query = (agg1
         .writeStream
         .outputMode("complete")
         .format("console")
         .foreachBatch(process_batch)
         .start()
        )
 
