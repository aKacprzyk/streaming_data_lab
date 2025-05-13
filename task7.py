from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("UserSegmentation").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),  
    StructField("timestamp", TimestampType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType())
])

stream = (
    spark.readStream
         .schema(schema)
         .json("data/stream")
)

segmented = (
    stream.withWatermark("timestamp", "1 minute")
          .groupBy(window("timestamp", "5 minutes"), col("user_id"))
          .agg(collect_set("event_type").alias("events"))
          .withColumn(
              "segment",
              expr("""
                  CASE 
                    WHEN array_contains(events, 'purchase') THEN 'Buyer'
                    WHEN array_contains(events, 'cart') THEN 'Cart abandoner'
                    ELSE 'Lurker'
                  END
              """)
          )
          .select("window", "user_id", "segment", "events")
)

query = (
    segmented.writeStream
             .format("console")
             .outputMode("append")
             .option("truncate", False)
             .start()
)

