"""
Spark Structured Streaming Consumer
Đọc dữ liệu từ Kafka, xử lý và ghi vào HDFS + MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

load_dotenv()

# Cấu hình
KAFKA_BOOTSTRAP = os.getenv('WSL2_IP', 'localhost') + ':9092'
KAFKA_TOPIC = 'house-listings'
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/bigdata_houses.listings")
HDFS_PATH = "hdfs://localhost:9000/bigdata/house-listings"
CHECKPOINT_PATH = "file:///tmp/spark-checkpoints"  # Dùng local thay vì HDFS

print(f"[CONFIG] Kafka: {KAFKA_BOOTSTRAP}")
print(f"[CONFIG] Topic: {KAFKA_TOPIC}")
print(f"[CONFIG] MongoDB: {MONGODB_URI}")
print(f"[CONFIG] HDFS: {HDFS_PATH}")

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("HouseListingsStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("[INFO] Spark Session initialized")

# 2. Define schema cho dữ liệu
schema = StructType([
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", LongType(), True),
    StructField("area_m2", DoubleType(), True),
    StructField("price_per_m2", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("street", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("property_type", StringType(), True),
    StructField("category", IntegerType(), True),
    StructField("post_time", LongType(), True),
    StructField("images", IntegerType(), True),
    StructField("crawl_timestamp", DoubleType(), True),
    StructField("source", StringType(), True)
])

# 3. Đọc stream từ Kafka
print("[INFO] Connecting to Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("[INFO] Connected to Kafka successfully")

# 4. Parse JSON từ Kafka message
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# 5. Data Cleaning & Transformations
cleaned_df = parsed_df \
    .filter(col("price").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("area_m2").isNotNull()) \
    .filter(col("area_m2") > 0) \
    .withColumn("processing_time", current_timestamp()) \
    .withColumn("price_billion", round(col("price") / 1000000000, 2)) \
    .withColumn("price_category", 
                when(col("price") < 1000000000, "< 1 tỷ")
                .when(col("price") < 3000000000, "1-3 tỷ")
                .when(col("price") < 5000000000, "3-5 tỷ")
                .when(col("price") < 10000000000, "5-10 tỷ")
                .otherwise("> 10 tỷ")) \
    .withColumn("area_category",
                when(col("area_m2") < 50, "< 50m²")
                .when(col("area_m2") < 100, "50-100m²")
                .when(col("area_m2") < 200, "100-200m²")
                .otherwise("> 200m²"))

print("[INFO] Data cleaning configured")

# 6. Write to HDFS (Parquet format) - Raw Data
query_hdfs_raw = cleaned_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"{HDFS_PATH}/raw") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/raw") \
    .trigger(processingTime="30 seconds") \
    .start()

print(f"[INFO] Writing to HDFS: {HDFS_PATH}/raw")

# 7. Aggregations - Tính giá trung bình theo quận
agg_by_district = cleaned_df \
    .groupBy(
        window(col("processing_time"), "5 minutes"),
        col("region"),
        col("district")
    ) \
    .agg(
        count("*").alias("total_listings"),
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        avg("area_m2").alias("avg_area"),
        avg("price_per_m2").alias("avg_price_per_m2")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Write aggregations to HDFS - Tạm comment vì Parquet không hỗ trợ Complete mode
# query_hdfs_agg = agg_by_district.writeStream \
#     .outputMode("complete") \
#     .format("parquet") \
#     .option("path", f"{HDFS_PATH}/aggregations") \
#     .option("checkpointLocation", f"{CHECKPOINT_PATH}/aggregations") \
#     .trigger(processingTime="1 minute") \
#     .start()
# print(f"[INFO] Writing aggregations to HDFS: {HDFS_PATH}/aggregations")

# 8. Write to MongoDB
def write_to_mongodb(batch_df, batch_id):
    """Ghi batch vào MongoDB (upsert để tránh duplicates)"""
    try:
        # Đếm số records trước khi insert
        total_records = batch_df.count()
        
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "bigdata_houses") \
            .option("collection", "listings") \
            .option("replaceDocument", "true") \
            .option("idFieldList", "id") \
            .option("ordered", "false") \
            .save()
        print(f"[SUCCESS] Batch {batch_id}: Processed {total_records} records to MongoDB (upsert mode)")
    except Exception as e:
        print(f"[ERROR] Batch {batch_id}: Failed to write to MongoDB: {e}")

query_mongo = cleaned_df.writeStream \
    .foreachBatch(write_to_mongodb) \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/mongodb") \
    .trigger(processingTime="30 seconds") \
    .start()

print("[INFO] Writing to MongoDB configured")

# 9. Console output để debug (optional)
query_console = cleaned_df.select(
    "id", "title", "price_billion", "area_m2", 
    "district", "price_category", "area_category"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .trigger(processingTime="1 minute") \
    .start()

print("[INFO] Console output enabled")

# 10. Await termination
print("\n" + "="*80)
print("🚀 Spark Streaming Started Successfully!")
print("="*80)
print(f"📊 Monitoring:")
print(f"   - Spark UI: http://localhost:4040")
print(f"   - HDFS UI: http://localhost:9870")
print(f"   - MongoDB: mongosh → use bigdata_houses → db.listings.find().limit(5)")
print("="*80)
print("\n⏳ Waiting for data from Kafka topic 'house-listings'...")
print("Press Ctrl+C to stop\n")

try:
    query_hdfs_raw.awaitTermination()
except KeyboardInterrupt:
    print("\n[INFO] Stopping Spark Streaming...")
    spark.stop()
    print("[INFO] Spark Streaming stopped")