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
