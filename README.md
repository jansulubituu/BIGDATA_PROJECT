# BIGDATA_PROJECT - Hệ thống xử lý dữ liệu bất động sản Real-time

Dự án xây dựng pipeline Big Data thu thập và xử lý dữ liệu bất động sản từ nhatot.com.

## 📐 Kiến trúc hệ thống

```
           ┌───────────────────┐
           │   Data Source      │
           │ nhatot.com Web     │
           │ Scraping/ Crawling│
           └─────────┬─────────┘
                     │
                     ▼
           ┌───────────────────   ┐
           │ Data Ingestion     │
           │ (Kafka Producer)   │
           │ Stream of Listings │
           └─────────┬────────────┘
                     │
                     ▼
           ┌───────────────────┐
           │ Stream Processing │ 
           │ (Apache Spark     │
           │ Structured Streaming) │
           │ - Clean data      │
           │ - Transformations │
           │ - UDFs, Aggregates│
           └─────────┬─────────┘
                     │
        ┌────────────┴─────────────┐
        │                          │
        ▼                          ▼
┌───────────────────┐       ┌───────────────────┐
│ Batch Storage     │       │ NoSQL Database    │
│ HDFS              │       │ MongoDB           │
│ Raw & Processed   │       │ Fast querying &   │
│ Data              │       │ analytics         │
└─────────┬─────────┘       └─────────┬─────────┘
          │                          │
          └────────────┬─────────────┘
                       ▼
               ┌───────────────┐
               │ Visualization │
               │ Dashboards    │
               │ (Grafana /    │
               │ Superset)     │
               └───────────────┘
```

## 📁 Cấu trúc Project

```
BIGDATA_PROJECT/
├── CrawlData.py              # Module crawl dữ liệu từ nhatot.com API
├── kafka_producer.py         # Kafka Producer - stream data vào Kafka ✅
├── kafka_consumer_test.py    # Test consumer để kiểm tra data trong Kafka
├── test_spark_streaming.py   # [CẦN TRIỂN KHAI] Spark Structured Streaming
├── requirements.txt          # Python dependencies
├── data_input/house/         # Dữ liệu đã crawl (backup)
│   └── 2025-12-12/          # 36+ JSON files
└── README.md                 # File này
```

## 🔧 Môi trường cần thiết

### Đã cài đặt trên WSL2:
- **Java 11** (`/usr/lib/jvm/java-11-openjdk-amd64`)
- **Kafka 3.6.1** (`/usr/local/kafka`)
- **Hadoop** (`/usr/local/hadoop`)
- **Spark** - Cần cài đặt cho Stream Processing

### Python Libraries:
```bash
pip install -r requirements.txt
# Cài đặt: requests, kafka-python, python-dotenv
```

---

##  Cấu hình môi trường (.env)

### Bước 1: Tạo file .env
```bash
# Copy file template
cp .env.example .env
```

### Bước 2: Lấy IP của WSL2
Trong WSL2 terminal, chạy:
```bash
hostname -I
# Ví dụ kết quả: 172.27.34.172
```

### Bước 3: Cập nhật file .env

Nội dung file `.env`:
```env
# IP của WSL2 (thay bằng IP thực tế của bạn)
WSL2_IP=172.27.34.172

# Kafka Configuration
KAFKA_PORT=9092
KAFKA_TOPIC=house-listings

# Producer Settings
CRAWL_LIMIT=50
BATCH_SIZE=10
```

**Lưu ý:** 
- Mỗi khi restart Windows, IP của WSL2 có thể thay đổi, cần cập nhật lại

---

## Phần 1: DATA INGESTION

### Khởi động Kafka Cluster

#### Terminal WSL 1 - Zookeeper:
```bash
cd /usr/local/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Terminal WSL 2 - Kafka:
```bash
cd /usr/local/kafka
bin/kafka-server-start.sh config/server.properties
```

#### Kiểm tra services:
```bash
jps
# Phải thấy:
# XXXX QuorumPeerMain (Zookeeper)
# YYYY Kafka
```

### Tạo Kafka Topic

```bash
kafka-topics.sh --create --topic house-listings \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Kiểm tra topic
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --describe --topic house-listings --bootstrap-server localhost:9092
```

###  Chạy Producer (từ Windows)

#### Bước 1: Cài đặt dependencies
```bash
pip install -r requirements.txt
```

#### Bước 2: Đảm bảo file .env đã cấu hình đúng
```bash
# Kiểm tra file .env có WSL2_IP đúng không
cat .env  # WSL2
# hoặc
type .env  # Windows CMD
# hoặc
Get-Content .env  # PowerShell
```

#### Bước 3: Chạy Producer
```bash
python kafka_producer.py
```

**Kết quả mong đợi:**
```
[CONFIG] Kafka Bootstrap: 172.27.34.172:9092
[CONFIG] Topic: house-listings
[CONFIG] Crawl Limit: 50, Batch Size: 10
[✓] Kafka connection successful!
[INFO] Kafka Producer initialized - Topic: house-listings
[INFO] Starting to crawl and stream 50 listings...
[INFO] Lấy được 50 list_id.
[1/50] Processing ID: 129940480
[SUCCESS] Sent ID: 129940480 → Topic: house-listings, Partition: 0, Offset: 0
...
[COMPLETED] Sent 50/50 listings to Kafka
```

**Nếu lỗi kết nối:**
- Kiểm tra Kafka đang chạy: `jps` trong WSL2
- Cập nhật lại `WSL2_IP` trong file `.env`
- Test connection: `Test-NetConnection -ComputerName <WSL2_IP> -Port 9092`

### Kiểm tra dữ liệu trong Kafka

#### Từ WSL2:
```bash
# Xem 5 messages đầu
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic house-listings \
  --from-beginning \
  --max-messages 5
```

#### Từ Windows:
```bash
python kafka_consumer_test.py
```

### Format dữ liệu trong Kafka

Mỗi message có format JSON:
```json
{
  "id": 129940480,
  "title": "Bán căn hộ 2PN...",
  "description": "...",
  "price": 3650000000,
  "area_m2": 123,
  "price_per_m2": 29674796.75,
  "region": "Hà Nội",
  "district": "Huyện Gia Lâm",
  "ward": "Thị trấn Trâu Quỳ",
  "street": "Yên Viên",
  "lat": 21.02097,
  "lng": 105.93817,
  "property_type": null,
  "category": 1010,
  "post_time": 1765506961000,
  "images": 12,
  "crawl_timestamp": 1735306123.456,
  "source": "nhatot.com"
}
```

---

## Phần 2: STREAM PROCESSING với Apache Spark

### Mục tiêu
Xây dựng Spark Structured Streaming để:
1. **Đọc dữ liệu real-time** từ Kafka topic `house-listings`
2. **Làm sạch và transform** dữ liệu
3. **Tính toán aggregations** (giá trung bình, số lượng tin đăng theo khu vực)
4. **Lưu trữ** vào HDFS (Parquet) và MongoDB

---

### Bước 1: Cài đặt Apache Spark (WSL2)

#### 1.1. Download và cài đặt Spark
```bash
cd /usr/local

# Download Spark 3.5.3 (phiên bản mới nhất, tương thích với Hadoop 3.x)
# Nếu link không hoạt động, kiểm tra phiên bản mới tại: https://spark.apache.org/downloads.html
sudo wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz

# Hoặc từ archive:
# sudo wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz

# Giải nén
sudo tar -xzf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 spark

# Xóa file tải về
sudo rm spark-3.5.3-bin-hadoop3.tgz

# Phân quyền
sudo chown -R $USER:$USER /usr/local/spark
```

#### 1.2. Cấu hình biến môi trường
```bash
# Mở file .bashrc
nano ~/.bashrc

# Thêm các dòng sau vào cuối file:
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3

# Lưu file (Ctrl+O, Enter, Ctrl+X)

# Load lại cấu hình
source ~/.bashrc
```

#### 1.3. Kiểm tra cài đặt
```bash
# Kiểm tra Spark version
spark-submit --version

# Kiểm tra PySpark
pyspark --version

# Test PySpark shell (Ctrl+D để thoát)
pyspark
```

**Kết quả mong đợi:**
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/
```

---

### Bước 2: Cài đặt Python Dependencies

**Lưu ý:** Ubuntu 22.04+ yêu cầu sử dụng virtual environment để tránh lỗi `externally-managed-environment`.

#### 2.1. Cài đặt python3-venv
```bash
sudo apt install python3-venv python3-full -y
```

#### 2.2. Tạo và kích hoạt virtual environment
```bash
# Tạo venv trong home directory
cd ~
python3 -m venv spark-venv

# Kích hoạt venv (dấu (spark-venv) sẽ xuất hiện ở đầu prompt)
source ~/spark-venv/bin/activate
```

#### 2.3. Cài đặt packages trong venv
```bash
# Cài đặt PySpark và PyMongo
pip install pyspark pymongo

# Verify cài đặt thành công
python -c "import pyspark; print(pyspark.__version__)"
python -c "import pymongo; print(pymongo.__version__)"
```

#### 2.4. Cấu hình PySpark sử dụng venv Python
```bash
# Mở file .bashrc
nano ~/.bashrc

# Tìm dòng: export PYSPARK_PYTHON=python3
# Thay bằng (thay 'donglam' bằng username của bạn):
export PYSPARK_PYTHON=/home/donglam/spark-venv/bin/python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Lưu file (Ctrl+O, Enter, Ctrl+X)

# Load lại cấu hình
source ~/.bashrc
```

#### 2.5. Test cài đặt
```bash
# Đảm bảo venv đang active
source ~/spark-venv/bin/activate

# Test PySpark
pyspark --version
```

**Kết quả mong đợi:**
```
Python 3.x.x
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/
```

**Lưu ý quan trọng:** Mỗi khi mở terminal mới để chạy Spark, nhớ activate venv:
```bash
source ~/spark-venv/bin/activate
```p

---

### Bước 3: Cài đặt và khởi động MongoDB (WSL2)

#### 3.1. Cài đặt MongoDB
```bash
# Import MongoDB GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor

# Thêm MongoDB repository
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
   sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# Update và cài đặt
sudo apt-get update
sudo apt-get install -y mongodb-org
```

#### 3.2. Khởi động MongoDB
```bash
# Start MongoDB service
sudo systemctl start mongod

# Enable auto-start
sudo systemctl enable mongod

# Kiểm tra status
sudo systemctl status mongod

# Test connection
mongosh
# Trong mongosh shell:
show dbs
exit
```

#### 3.3. Tạo database và collection
```bash
mongosh
```

Trong MongoDB shell:
```javascript
// Tạo database
use bigdata_houses

// Tạo collection với index
db.createCollection("listings")

// Tạo index cho performance
db.listings.createIndex({ "id": 1 }, { unique: true })
db.listings.createIndex({ "region": 1, "district": 1 })
db.listings.createIndex({ "price": 1 })
db.listings.createIndex({ "crawl_timestamp": -1 })

// Kiểm tra
show collections
db.listings.getIndexes()

exit
```

---

### Bước 4: Khởi động HDFS

#### 4.0. Cấu hình SSH và Hadoop (chạy lần đầu tiên)

**A. Cài đặt và cấu hình SSH:**
```bash
# 1. Cài đặt SSH server
sudo apt install openssh-server -y

# 2. Khởi động SSH service
sudo service ssh start

# 3. Kiểm tra SSH đang chạy
sudo service ssh status

# 4. Tạo SSH key (cho passwordless SSH)
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa

# 5. Copy public key vào authorized_keys
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# 6. Test SSH đến localhost
ssh localhost
# Gõ "yes" khi được hỏi, sau đó gõ "exit" để thoát
```

**B. Cấu hình Hadoop core-site.xml:**
```bash
# Backup file cũ
sudo cp /usr/local/hadoop/etc/hadoop/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml.backup

# Sửa file
sudo nano /usr/local/hadoop/etc/hadoop/core-site.xml
```

Nội dung file `core-site.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-${user.name}</value>
    </property>
</configuration>
```

**C. Kiểm tra hdfs-site.xml:**
```bash
sudo nano /usr/local/hadoop/etc/hadoop/hdfs-site.xml
```

Nội dung file `hdfs-site.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

#### 4.1. Format NameNode (chỉ lần đầu tiên)
```bash
# Chỉ chạy lần đầu hoặc khi cần reset HDFS
hdfs namenode -format -force
```

**Kết quả mong đợi:**
```
...
INFO namenode.FSImageFormatProtobuf: Image file ... saved in 0 seconds
INFO namenode.NNStorageRetentionManager: Going to retain 1 images with txid >= 0
INFO namenode.FSNamesystem: Stopping services started for active state
SHUTDOWN_MSG: Shutting down NameNode at ...
```

#### 4.2. Khởi động HDFS
```bash
# Start HDFS services
start-dfs.sh

# Kiểm tra services
jps
# Phải thấy:
# - NameNode
# - DataNode
# - SecondaryNameNode
# - QuorumPeerMain (Zookeeper)
# - Kafka
```

**Lưu ý:** 
- Warning "Unable to load native-hadoop library" là bình thường trên WSL2, không ảnh hưởng hoạt động
- Warning "Cannot set priority" cũng không ảnh hưởng

#### 4.3. Tạo thư mục trong HDFS
```bash
# Tạo thư mục cho dữ liệu
hdfs dfs -mkdir -p /bigdata/house-listings/raw
hdfs dfs -mkdir -p /bigdata/house-listings/processed
hdfs dfs -mkdir -p /bigdata/checkpoints

# Kiểm tra
hdfs dfs -ls /bigdata
hdfs dfs -ls /bigdata/house-listings
```

#### 4.4. Truy cập HDFS Web UI
Mở trình duyệt: http://localhost:9870

---

### 💻 Bước 5: Tạo Spark Streaming Script

Tạo file `spark_streaming_consumer.py`:

```python
"""
Spark Structured Streaming Consumer
Đọc dữ liệu từ Kafka, xử lý và ghi vào HDFS + MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Cấu hình
KAFKA_BOOTSTRAP = os.getenv('WSL2_IP', 'localhost') + ':9092'
KAFKA_TOPIC = 'house-listings'
MONGODB_URI = "mongodb://localhost:27017/bigdata_houses.listings"
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

# Write aggregations to HDFS
# Note: Tạm comment vì Parquet không hỗ trợ Complete mode
# Sẽ cần thêm watermark và dùng append mode để fix
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
```

---

### 🚀 Bước 6: Chạy Spark Streaming

#### 6.1. Đảm bảo các services đang chạy
```bash
# Kiểm tra tất cả services
jps

# Phải thấy:
# - QuorumPeerMain (Zookeeper)
# - Kafka
# - NameNode (HDFS)
# - DataNode (HDFS)

# Kiểm tra MongoDB
sudo systemctl status mongod
```

#### 6.2. Chạy Spark Streaming
```bash
# Bước 1: Activate virtual environment
source ~/spark-venv/bin/activate

# Bước 2: Load biến môi trường
source ~/.bashrc

# Bước 3: Chạy Spark Streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  spark_streaming_consumer.py
```

**Kết quả mong đợi:**
```
[CONFIG] Kafka: 172.27.34.172:9092
[CONFIG] Topic: house-listings
[INFO] Spark Session initialized
[INFO] Connecting to Kafka...
[INFO] Connected to Kafka successfully
[INFO] Data cleaning configured
[INFO] Writing to HDFS: hdfs://localhost:9000/bigdata/house-listings/raw
[INFO] Writing aggregations to HDFS
[INFO] Writing to MongoDB configured

================================================================================
🚀 Spark Streaming Started Successfully!
================================================================================
📊 Monitoring:
   - Spark UI: http://localhost:4040
   - HDFS UI: http://localhost:9870
   - MongoDB: mongosh → use bigdata_houses → db.listings.find().limit(5)
================================================================================

⏳ Waiting for data from Kafka topic 'house-listings'...
```

#### 6.3. Trong terminal khác, chạy Producer để gửi data
```bash
# Windows CMD/PowerShell
python kafka_producer.py
```

---

### 📊 Bước 7: Monitoring và Kiểm tra kết quả

#### 7.1. Spark UI
Mở trình duyệt: http://localhost:4040
- Xem Streaming tab
- Kiểm tra Input Rate, Processing Time
- Xem các stages và tasks

#### 7.2. Kiểm tra dữ liệu trong HDFS
```bash
# Xem cấu trúc thư mục
hdfs dfs -ls /bigdata/house-listings
hdfs dfs -ls /bigdata/house-listings/raw
hdfs dfs -ls /bigdata/house-listings/aggregations

# Đếm số files
hdfs dfs -count /bigdata/house-listings/raw

# Xem nội dung file (lấy 1 file bất kỳ)
hdfs dfs -cat /bigdata/house-listings/raw/part-*.parquet | head -100
```

#### 7.3. Kiểm tra dữ liệu trong MongoDB
```bash
mongosh
```

Trong MongoDB shell:
```javascript
use bigdata_houses

// Đếm số documents
db.listings.countDocuments()

// Xem 5 records mới nhất
db.listings.find().sort({crawl_timestamp: -1}).limit(5).pretty()

// Thống kê theo quận
db.listings.aggregate([
  { $group: {
      _id: "$district",
      count: { $sum: 1 },
      avg_price: { $avg: "$price" },
      avg_area: { $avg: "$area_m2" }
  }},
  { $sort: { count: -1 } },
  { $limit: 10 }
])

// Tìm nhà giá > 5 tỷ
db.listings.find({ price: { $gt: 5000000000 } }).limit(5).pretty()

exit
```

#### 7.4. Kiểm tra Kafka Consumer Group
```bash
# Xem consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Xem chi tiết lag của group
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group spark-kafka-streaming
```

---

### 🛠️ Troubleshooting Stream Processing

#### Lỗi: Checkpoint corrupt (Error reading delta file)

**Nguyên nhân:** Spark Streaming bị dừng đột ngột hoặc HDFS checkpoint bị lỗi.

**Giải pháp:**
```bash
# 1. Dừng Spark Streaming (Ctrl+C)
# Đợi shutdown hoàn toàn (~10 giây)

# 2. Xóa checkpoint
rm -rf /tmp/spark-checkpoints

# 3. Chạy lại Spark Streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --master local[*] \
  --driver-memory 2g \
  --executor-memory 2g \
  spark_streaming_consumer.py
```

**Lưu ý:** Script hiện dùng local checkpoint (`file:///tmp/spark-checkpoints`) thay vì HDFS để tránh lỗi này.

#### Lỗi: MongoDB duplicate key (E11000)

**Nguyên nhân:** Spark đọc lại data cũ từ Kafka và cố insert vào MongoDB.

**Giải pháp:** Script đã config upsert mode với `replaceDocument=true` và `idFieldList=id`. Nếu vẫn lỗi:
```bash
# Option 1: Xóa data cũ trong MongoDB
mongosh
use bigdata_houses
db.listings.deleteMany({})
exit

# Option 2: Xóa checkpoint và chạy lại
rm -rf /tmp/spark-checkpoints
```

#### Lỗi: HDFS không khởi động (NameNode/DataNode không xuất hiện trong jps)

**Nguyên nhân:** File `core-site.xml` chưa cấu hình hoặc cấu hình sai.

**Giải pháp:**
```bash
# 1. Xem log để tìm lỗi
tail -50 /usr/local/hadoop/logs/hadoop-*-namenode-*.log

# 2. Nếu thấy lỗi "Invalid URI for NameNode address (check fs.defaultFS): file:/// has no authority"
# Kiểm tra và sửa core-site.xml (xem Bước 4.0.B)

# 3. Format lại NameNode
hdfs namenode -format -force

# 4. Start lại HDFS
start-dfs.sh
jps
```

#### Lỗi: SSH connection refused khi start HDFS

**Nguyên nhân:** SSH service chưa chạy hoặc chưa cấu hình passwordless SSH.

**Giải pháp:**
```bash
# 1. Start SSH service
sudo service ssh start

# 2. Kiểm tra SSH
sudo service ssh status

# 3. Nếu chưa có SSH key, tạo mới (xem Bước 4.0.A)
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# 4. Test SSH
ssh localhost
```

#### Lỗi: Kafka topic bị mất sau khi restart

**Nguyên nhân:** Kafka data directory mặc định ở `/tmp/kafka-logs` có thể bị xóa khi reboot.

**Giải pháp - Đổi Kafka data directory sang vị trí cố định:**
```bash
# 1. Tạo thư mục lưu trữ vĩnh viễn
sudo mkdir -p /usr/local/kafka/data
sudo chown -R $USER:$USER /usr/local/kafka/data

# 2. Sửa file config
nano /usr/local/kafka/config/server.properties

# 3. Tìm dòng: log.dirs=/tmp/kafka-logs
# Thay bằng: log.dirs=/usr/local/kafka/data

# 4. Tương tự cho Zookeeper
sudo mkdir -p /usr/local/kafka/zookeeper-data
sudo chown -R $USER:$USER /usr/local/kafka/zookeeper-data

nano /usr/local/kafka/config/zookeeper.properties
# Tìm: dataDir=/tmp/zookeeper
# Thay: dataDir=/usr/local/kafka/zookeeper-data

# 5. Restart Kafka cluster
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
sleep 5
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5
bin/kafka-server-start.sh config/server.properties &

# 6. Tạo lại topic (chỉ lần đầu sau khi đổi directory)
kafka-topics.sh --create --topic house-listings \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

**Sau khi cấu hình, topic sẽ persistent vĩnh viễn.**

#### Lỗi: Kafka connection timeout
```bash
# Kiểm tra Kafka đang chạy
jps | grep Kafka

# Kiểm tra topic
kafka-topics.sh --list --bootstrap-server localhost:9092

# Test connection
nc -zv localhost 9092
```

#### Lỗi: HDFS connection refused
```bash
# Kiểm tra HDFS
jps | grep -E "NameNode|DataNode"

# Khởi động HDFS nếu cần
start-dfs.sh

# Kiểm tra safe mode
hdfs dfsadmin -safemode get

# Thoát safe mode nếu stuck
hdfs dfsadmin -safemode leave
```

#### Lỗi: MongoDB connection failed
```bash
# Kiểm tra MongoDB status
sudo systemctl status mongod

# Khởi động nếu cần
sudo systemctl start mongod

# Xem logs
sudo tail -f /var/log/mongodb/mongod.log
```

#### Lỗi: OutOfMemoryError trong Spark
```bash
# Tăng memory khi chạy spark-submit
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  ...
```

#### Xem Spark Streaming logs
```bash
# Logs được ghi vào console
# Hoặc xem trong Spark UI: http://localhost:4040
```

---

### 📈 Nâng cao: Thêm Aggregations và Analytics

Thêm vào file `spark_streaming_consumer.py`:

```python
# Aggregation theo price_category
price_stats = cleaned_df \
    .groupBy(
        window(col("processing_time"), "10 minutes"),
        col("price_category")
    ) \
    .agg(
        count("*").alias("count"),
        avg("price").alias("avg_price")
    )

# Aggregation theo region
region_stats = cleaned_df \
    .groupBy(
        window(col("processing_time"), "10 minutes"),
        col("region")
    ) \
    .agg(
        count("*").alias("total"),
        avg("price").alias("avg_price"),
        avg("area_m2").alias("avg_area")
    )

# Write to separate paths
price_stats.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", f"{HDFS_PATH}/stats_price") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/stats_price") \
    .start()

region_stats.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", f"{HDFS_PATH}/stats_region") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/stats_region") \
    .start()
```

---

## 🔄 Quản lý Services

### Thứ tự khởi động Services

**Thứ tự đúng khi bật tất cả services:**

```bash
# 1. SSH service (nếu chưa chạy)
sudo service ssh start

# 2. Zookeeper (phải chạy trước Kafka)
cd /usr/local/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties &

# 3. Kafka (terminal mới, sau khi Zookeeper đã chạy ổn định ~5s)
sleep 5
cd /usr/local/kafka
bin/kafka-server-start.sh config/server.properties &

# 4. HDFS (terminal mới)
start-dfs.sh

# 5. MongoDB
sudo systemctl start mongod

# 6. Kiểm tra tất cả services
jps
sudo systemctl status mongod
```

### Thứ tự tắt Services (ngược lại với thứ tự bật)

**Cách tắt an toàn:**

```bash
# 1. Dừng Spark Streaming (nếu đang chạy)
# Trong terminal đang chạy spark-submit, nhấn Ctrl+C

# 2. Dừng HDFS
stop-dfs.sh

# 3. Dừng MongoDB
sudo systemctl stop mongod

# 4. Dừng Kafka
cd /usr/local/kafka
bin/kafka-server-stop.sh

# 5. Dừng Zookeeper (cuối cùng)
bin/zookeeper-server-stop.sh

# 6. Kiểm tra đã tắt hết chưa
jps
# Chỉ thấy Jps là OK
```

### Kiểm tra trạng thái Services

```bash
# Kiểm tra tất cả Java processes
jps
# Output mong đợi khi đang chạy:
# - QuorumPeerMain (Zookeeper)
# - Kafka
# - NameNode
# - DataNode
# - SecondaryNameNode

# Kiểm tra MongoDB
sudo systemctl status mongod

# Kiểm tra SSH
sudo service ssh status

# Kiểm tra port đang được sử dụng
netstat -tuln | grep -E "2181|9092|9000|9870|27017"
# 2181  - Zookeeper
# 9092  - Kafka
# 9000  - HDFS
# 9870  - HDFS Web UI
# 27017 - MongoDB
```

### Khởi động lại một Service cụ thể

#### Khởi động lại Kafka
```bash
# Dừng
cd /usr/local/kafka
bin/kafka-server-stop.sh

# Đợi ~5 giây
sleep 5

# Bật lại
bin/kafka-server-start.sh config/server.properties &
```

#### Khởi động lại Zookeeper
```bash
# Lưu ý: Nếu restart Zookeeper, phải restart Kafka sau đó

# Dừng Kafka trước
cd /usr/local/kafka
bin/kafka-server-stop.sh

# Dừng Zookeeper
bin/zookeeper-server-stop.sh

# Đợi ~5 giây
sleep 5

# Bật lại Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Đợi Zookeeper ổn định
sleep 5

# Bật lại Kafka
bin/kafka-server-start.sh config/server.properties &
```

#### Khởi động lại HDFS
```bash
# Dừng
stop-dfs.sh

# Bật lại
start-dfs.sh

# Kiểm tra
jps
hdfs dfs -ls /
```

#### Khởi động lại MongoDB
```bash
# Dừng
sudo systemctl stop mongod

# Bật lại
sudo systemctl start mongod

# Kiểm tra
sudo systemctl status mongod
```

### Script tự động (tùy chọn)

Tạo file `start_all.sh`:
```bash
#!/bin/bash
echo "🚀 Starting all services..."

# SSH
sudo service ssh start
echo "✅ SSH started"

# Zookeeper
cd /usr/local/kafka
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
echo "✅ Zookeeper started"
sleep 5

# Kafka
nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
echo "✅ Kafka started"
sleep 3

# HDFS
start-dfs.sh
echo "✅ HDFS started"

# MongoDB
sudo systemctl start mongod
echo "✅ MongoDB started"

echo ""
echo "📊 Services status:"
jps
echo ""
sudo systemctl status mongod --no-pager
```

Tạo file `stop_all.sh`:
```bash
#!/bin/bash
echo "🛑 Stopping all services..."

# HDFS
stop-dfs.sh
echo "✅ HDFS stopped"

# MongoDB
sudo systemctl stop mongod
echo "✅ MongoDB stopped"

# Kafka
cd /usr/local/kafka
bin/kafka-server-stop.sh
echo "✅ Kafka stopped"
sleep 3

# Zookeeper
bin/zookeeper-server-stop.sh
echo "✅ Zookeeper stopped"

echo ""
echo "📊 Remaining processes:"
jps
```

Phân quyền thực thi:
```bash
chmod +x start_all.sh stop_all.sh
```

Sử dụng:
```bash
# Bật tất cả
./start_all.sh

# Tắt tất cả
./stop_all.sh
```

---

### ✅ Checklist Stream Processing

- [ ] Cài đặt Apache Spark
- [ ] Cấu hình biến môi trường (SPARK_HOME, PATH)
- [ ] Cài đặt MongoDB
- [ ] Khởi động HDFS và tạo thư mục
- [ ] Tạo database và collection trong MongoDB
- [ ] Tạo file `spark_streaming_consumer.py`
- [ ] Chạy Spark Streaming
- [ ] Chạy Producer để test
- [ ] Kiểm tra Spark UI (http://localhost:4040)
- [ ] Kiểm tra dữ liệu trong HDFS
- [ ] Kiểm tra dữ liệu trong MongoDB
- [ ] Verify không còn lỗi checkpoint/duplicate

### 🎯 Kết quả mong đợi

**Spark Streaming chạy thành công khi thấy:**
```
[SUCCESS] Batch 0: Processed X records to MongoDB (upsert mode)
[SUCCESS] Batch 1: Processed X records to MongoDB (upsert mode)
```

**Không có lỗi:**
- ❌ Checkpoint corrupt errors
- ❌ E11000 duplicate key errors
- ❌ HDFS connection refused

**Warnings có thể ignore:**
- ⚠️ `Unable to load native-hadoop library` (bình thường trên WSL2)
- ⚠️ `CaseInsensitiveStringMap: Converting duplicated key` (không ảnh hưởng)
- ⚠️ `Current batch is falling behind` (bình thường với batch đầu tiên)

### 📝 Lưu ý quan trọng

1. **startingOffsets="latest"**: Chỉ xử lý data MỚI, tránh đọc lại data cũ
2. **Local checkpoint**: Dùng `file:///tmp/spark-checkpoints` thay vì HDFS (ổn định hơn)
3. **MongoDB upsert**: Config `replaceDocument=true` để tự động update thay vì lỗi duplicate
4. **Không dùng dropDuplicates()**: Gây lỗi checkpoint, dùng MongoDB unique index thay thế
5. **Dừng Spark an toàn**: Nhấn Ctrl+C một lần và đợi shutdown hoàn toàn (tránh corrupt checkpoint)

---
