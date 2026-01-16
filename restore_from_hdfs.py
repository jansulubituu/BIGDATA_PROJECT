"""
Script khôi phục dữ liệu từ HDFS vào MongoDB
Đọc Parquet files từ HDFS và import vào MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv

load_dotenv()

# Cấu hình
HDFS_PATH = "hdfs://localhost:9000/bigdata/house-listings/raw"
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "bigdata_houses")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "listings")

print("=" * 80)
print("🔧 CÔNG CỤ KHÔI PHỤC DỮ LIỆU TỪ HDFS VÀO MONGODB")
print("=" * 80)
print(f"📂 HDFS Path: {HDFS_PATH}")
print(f"🗄️  MongoDB: {MONGODB_URI}")
print(f"📊 Database: {MONGODB_DATABASE}")
print(f"📋 Collection: {MONGODB_COLLECTION}")
print("=" * 80)

# 1. Khởi tạo Spark Session
print("\n[1/5] Khởi tạo Spark Session...")
spark = SparkSession.builder \
    .appName("RestoreFromHDFS") \
    .config("spark.jars.packages", 
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", 
            f"{MONGODB_URI}") \
    .config("spark.mongodb.write.database", MONGODB_DATABASE) \
    .config("spark.mongodb.write.collection", MONGODB_COLLECTION) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("   ✅ Spark Session initialized")

# 2. Kiểm tra dữ liệu trong HDFS
print("\n[2/5] Kiểm tra dữ liệu trong HDFS...")
try:
    # Đọc Parquet từ HDFS
    df = spark.read.parquet(HDFS_PATH)
    total_records = df.count()
    
    print(f"   ✅ Tìm thấy {total_records:,} records trong HDFS")
    
    # Hiển thị schema
    print("\n   📋 Schema:")
    df.printSchema()
    
    # Hiển thị mẫu dữ liệu
    print("\n   🔍 Mẫu 3 records đầu tiên:")
    df.show(3, truncate=False)
    
except Exception as e:
    print(f"   ❌ Lỗi đọc HDFS: {e}")
    print("\n💡 Kiểm tra:")
    print("   1. HDFS đang chạy: jps | grep NameNode")
    print("   2. Đường dẫn đúng: hdfs dfs -ls /bigdata/house-listings/raw")
    spark.stop()
    exit(1)

# 3. Kiểm tra duplicates
print("\n[3/5] Kiểm tra duplicates...")
total_records = df.count()
unique_ids = df.select("id").distinct().count()
duplicates = total_records - unique_ids

if duplicates > 0:
    print(f"   ⚠️  Tìm thấy {duplicates} records trùng lặp")
    print(f"   🔧 Đang loại bỏ duplicates...")
    df = df.dropDuplicates(["id"])
    print(f"   ✅ Sau khi loại bỏ: {df.count():,} records")
else:
    print(f"   ✅ Không có duplicates")

# 4. Thống kê trước khi import
print("\n[4/5] Thống kê dữ liệu:")
print(f"   📊 Tổng records: {df.count():,}")
print(f"   📍 Số vùng: {df.select('region').distinct().count()}")
print(f"   🏘️  Số quận/huyện: {df.select('district').distinct().count()}")

# Phân bố theo vùng
print("\n   🗺️  Phân bố theo vùng:")
region_counts = df.groupBy("region").count().orderBy(col("count").desc())
region_counts.show(truncate=False)

# 5. Xác nhận trước khi import
print("\n[5/5] Chuẩn bị import vào MongoDB...")
print("=" * 80)
print("⚠️  CẢNH BÁO: Dữ liệu cũ trong MongoDB sẽ bị XÓA!")
print("=" * 80)
confirm = input("👉 Tiếp tục? (yes/no): ").strip().lower()

if confirm != "yes":
    print("❌ Đã hủy")
    spark.stop()
    exit(0)

# Import vào MongoDB
print("\n🚀 Đang import vào MongoDB...")

try:
    # Xóa collection cũ (dùng PyMongo vì Spark không có delete API)
    from pymongo import MongoClient
    
    # Parse MongoDB URI
    if MONGODB_URI.startswith("mongodb+srv://") or MONGODB_URI.startswith("mongodb://"):
        client = MongoClient(MONGODB_URI)
    else:
        # Nếu là local connection string
        client = MongoClient(f"mongodb://{MONGODB_URI.split('/')[-1]}")
    
    db = client[MONGODB_DATABASE]
    collection = db[MONGODB_COLLECTION]
    
    print(f"   🗑️  Đang xóa collection cũ...")
    delete_result = collection.delete_many({})
    print(f"   ✅ Đã xóa {delete_result.deleted_count:,} documents cũ")
    
    # Write vào MongoDB bằng Spark
    print(f"   📥 Đang import {df.count():,} records...")
    
    df.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", MONGODB_DATABASE) \
        .option("collection", MONGODB_COLLECTION) \
        .option("ordered", "false") \
        .save()
    
    print("   ✅ Import thành công!")
    
    # Verify
    print("\n📊 Verifying...")
    final_count = collection.count_documents({})
    print(f"   ✅ MongoDB hiện có: {final_count:,} documents")
    
    # Hiển thị mẫu document
    print("\n   🔍 Mẫu document:")
    sample = collection.find_one()
    if sample:
        import json
        print(json.dumps(sample, indent=2, default=str, ensure_ascii=False))
    
    # Tạo indexes để tối ưu performance
    print("\n   🔧 Tạo indexes...")
    collection.create_index([("id", 1)], unique=True, background=True)
    collection.create_index([("region", 1), ("district", 1)], background=True)
    collection.create_index([("price", 1)], background=True)
    collection.create_index([("crawl_timestamp", -1)], background=True)
    print("   ✅ Indexes created")
    
    # Thống kê cuối cùng
    print("\n" + "=" * 80)
    print("🎉 HOÀN TẤT KHÔI PHỤC DỮ LIỆU!")
    print("=" * 80)
    print(f"📊 Tổng số documents: {final_count:,}")
    print(f"📍 Số vùng: {len(collection.distinct('region'))}")
    print(f"🏘️  Số quận/huyện: {len(collection.distinct('district'))}")
    
    print("\n🗺️  Phân bố theo vùng:")
    pipeline = [
        {"$group": {"_id": "$region", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    for doc in collection.aggregate(pipeline):
        print(f"   - {doc['_id']}: {doc['count']:,} listings")
    
    client.close()
    
except Exception as e:
    print(f"   ❌ Lỗi: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\n" + "=" * 80)
    print("🏁 KẾT THÚC")
    print("=" * 80)
