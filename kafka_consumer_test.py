"""
Kafka Consumer Test - Đọc và hiển thị dữ liệu từ Kafka topic
Dùng để kiểm tra xem Producer có gửi dữ liệu thành công không
"""

import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def consume_messages(topic='house-listings', 
                     bootstrap_servers='localhost:9092',
                     max_messages=10):
    """
    Đọc và hiển thị messages từ Kafka topic
    
    Args:
        topic: Tên topic cần đọc
        bootstrap_servers: Địa chỉ Kafka broker
        max_messages: Số message tối đa cần đọc (0 = unlimited)
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',  # Đọc từ đầu topic
            enable_auto_commit=True,
            group_id='test-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=10000  # Timeout sau 10s không có message mới
        )
        
        print(f"[INFO] Consuming messages from topic: {topic}")
        print("=" * 80)
        
        count = 0
        for message in consumer:
            count += 1
            
            print(f"\n[Message #{count}]")
            print(f"Key: {message.key}")
            print(f"Partition: {message.partition}, Offset: {message.offset}")
            print(f"Timestamp: {message.timestamp}")
            print(f"Value:")
            print(json.dumps(message.value, indent=2, ensure_ascii=False))
            print("-" * 80)
            
            if max_messages > 0 and count >= max_messages:
                break
        
        consumer.close()
        print(f"\n[COMPLETED] Read {count} messages")
        
    except KafkaError as e:
        print(f"[ERROR] Kafka error: {e}")
    except Exception as e:
        print(f"[ERROR] {e}")


def get_topic_info(topic='house-listings', bootstrap_servers='localhost:9092'):
    """Hiển thị thông tin về topic"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id='info-consumer'
        )
        
        partitions = consumer.partitions_for_topic(topic)
        if partitions is None:
            print(f"[ERROR] Topic '{topic}' không tồn tại")
            return
        
        print(f"[INFO] Topic: {topic}")
        print(f"Partitions: {partitions}")
        
        # Lấy offset của từng partition
        from kafka import TopicPartition
        for partition_id in partitions:
            tp = TopicPartition(topic, partition_id)
            consumer.assign([tp])
            
            # Offset đầu và cuối
            consumer.seek_to_beginning(tp)
            beginning = consumer.position(tp)
            
            consumer.seek_to_end(tp)
            end = consumer.position(tp)
            
            total_messages = end - beginning
            print(f"  Partition {partition_id}: {total_messages} messages (offset {beginning} → {end})")
        
        consumer.close()
        
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    import sys
    
    topic = 'house-listings'
    
    # Hiển thị thông tin topic
    print("=" * 80)
    get_topic_info(topic)
    print("=" * 80)
    
    # Đọc messages
    print("\nPress Ctrl+C to stop...")
    try:
        # Đọc 20 messages đầu tiên
        consume_messages(topic, max_messages=20)
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user")
