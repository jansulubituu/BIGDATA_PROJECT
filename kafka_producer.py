"""
Kafka Producer - Gửi dữ liệu bất động sản vào Kafka topic
"""

import json
import time
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from CrawlData import extract_house, fetch_house_ids, fetch_house_detail, extract_one

# Load environment variables từ file .env
load_dotenv()


class HouseDataProducer:
    """Producer gửi dữ liệu nhà đất vào Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='house-listings'):
        """
        Khởi tạo Kafka Producer
        
        Args:
            bootstrap_servers: Địa chỉ Kafka broker
            topic: Tên topic để gửi dữ liệu
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Đảm bảo message được ghi thành công
            retries=3,
            max_in_flight_requests_per_connection=1,  # Đảm bảo thứ tự message
            compression_type='gzip'  # Nén dữ liệu
        )
        print(f"[INFO] Kafka Producer initialized - Topic: {topic}")
    
    def send_message(self, key, value):
        """
        Gửi 1 message vào Kafka
        
        Args:
            key: ID của tin đăng (dùng làm partition key)
            value: Dict chứa dữ liệu nhà đất
        """
        try:
            future = self.producer.send(
                self.topic,
                key=key,
                value=value
            )
            
            # Đợi confirm từ Kafka
            record_metadata = future.get(timeout=10)
            
            print(f"[SUCCESS] Sent ID: {key} → Topic: {record_metadata.topic}, "
                  f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            print(f"[ERROR] Failed to send ID {key}: {e}")
            return False
    
    def crawl_and_stream(self, limit_rows=300, batch_size=10):
        """
        Crawl dữ liệu và stream vào Kafka theo batch
        
        Args:
            limit_rows: Số lượng tin đăng cần crawl
            batch_size: Số message gửi mỗi batch rồi flush
        """
        print(f"[INFO] Starting to crawl and stream {limit_rows} listings...")
        
        # Lấy danh sách ID
        ids = fetch_house_ids(limit_ids=limit_rows)
        total = len(ids)
        success_count = 0
        
        # Crawl và gửi từng tin
        for idx, ad_id in enumerate(ids, 1):
            print(f"\n[{idx}/{total}] Processing ID: {ad_id}")
            
            # Crawl chi tiết
            raw_data = fetch_house_detail(ad_id)
            clean_data = extract_one(raw_data)
            
            if clean_data:
                # Thêm metadata
                clean_data['crawl_timestamp'] = time.time()
                clean_data['source'] = 'nhatot.com'
                
                # Gửi vào Kafka
                if self.send_message(key=ad_id, value=clean_data):
                    success_count += 1
            else:
                print(f"[WARN] No data for ID: {ad_id}")
            
            # Flush sau mỗi batch để đảm bảo data được gửi
            if idx % batch_size == 0:
                self.producer.flush()
                print(f"[INFO] Flushed batch {idx//batch_size} ({success_count}/{idx} success)")
            
            time.sleep(0.3)  # Tránh spam API
        
        # Flush cuối cùng
        self.producer.flush()
        
        print(f"\n[COMPLETED] Sent {success_count}/{total} listings to Kafka")
        return success_count
    
    def close(self):
        """Đóng producer"""
        self.producer.close()
        print("[INFO] Kafka Producer closed")


def test_connection(bootstrap_servers='localhost:9092'):
    """Kiểm tra kết nối đến Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=5000
        )
        producer.close()
        print("[✓] Kafka connection successful!")
        return True
    except Exception as e:
        print(f"[Kafka connection failed: {e}")
        print("\nHướng dẫn:")
        print("1. Đảm bảo Kafka đang chạy trên WSL2")
        print("2. Kiểm tra port 9092 đã mở")
        print("3. Trong WSL2 chạy: kafka-topics.sh --list --bootstrap-server localhost:9092")
        return False


if __name__ == "__main__":
    # Đọc cấu hình từ .env file
    WSL2_IP = os.getenv('WSL2_IP', 'localhost')
    KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'house-listings')
    CRAWL_LIMIT = int(os.getenv('CRAWL_LIMIT', '50'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    
    bootstrap_servers = f'{WSL2_IP}:{KAFKA_PORT}'
    
    print(f"[CONFIG] Kafka Bootstrap: {bootstrap_servers}")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC}")
    print(f"[CONFIG] Crawl Limit: {CRAWL_LIMIT}, Batch Size: {BATCH_SIZE}")
    
    # Test kết nối
    if not test_connection(bootstrap_servers=bootstrap_servers):
        print("\n[TIP] Cập nhật WSL2_IP trong file .env")
        print("[TIP] Lấy IP bằng lệnh: hostname -I (trong WSL2)")
        exit(1)
    
    # Khởi tạo producer
    producer = HouseDataProducer(
        bootstrap_servers=bootstrap_servers,
        topic=KAFKA_TOPIC
    )
    
    try:
        # Crawl và stream
        producer.crawl_and_stream(limit_rows=CRAWL_LIMIT, batch_size=BATCH_SIZE)
    finally:
        producer.close()
