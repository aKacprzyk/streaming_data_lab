batch_counter = {"count": 0}

def process_batch(df, batch_id):
    batch_counter["count"] += 1
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)
    if batch_counter["count"] % 7 == 0:
        spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

rate_df = (spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load())
 
events = (rate_df.withColumn("user_id", expr("concat('u', cast(rand()*100 as int))") )
          .withColumn("event_type", expr("case when rand() > 0.7 then 'purchase' else 'view' end") )
          .select("timestamp", "user_id", "event_type")
         )

purchases = events.filter(col("event_type") == "purchase") \
                  .withColumn("info", expr("concat('event:', purchase)"))

query = (purchases.writeStream
         .format("console")
         .outputMode("append")
         .foreachBatch(process_batch)
         .start())
