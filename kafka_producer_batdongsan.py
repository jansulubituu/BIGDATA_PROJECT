"""
Kafka Producer - Gửi dữ liệu bất động sản từ batdongsan.com.vn vào Kafka topic
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import selenium.webdriver.support.expected_conditions as EC
import undetected_chromedriver as uc
import time
import numpy as np
import pandas as pd
from urllib.parse import urlparse, parse_qs, unquote
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load environment variables từ file .env
load_dotenv()


def get_hrefs_from_page(driver, url, base_domain="https://batdongsan.com.vn"):
    """Lấy danh sách href từ một trang"""
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "js__product-link-for-product-id")))
    elements = driver.find_elements(By.CLASS_NAME, "js__product-link-for-product-id")
    
    hrefs = []
    for element in elements:
        href = element.get_attribute("href")
        # Chuyển relative URL thành absolute URL
        if href and not href.startswith(('http://', 'https://')):
            href = base_domain + href
        hrefs.append(href)
    return hrefs


def crawl_hrefs_batdongsan(max_pages=1, output_file="hrefs.txt"):
    """
    Crawl danh sách URL bất động sản từ batdongsan.com.vn
    
    Args:
        max_pages: Số trang tối đa cần crawl
        output_file: File lưu kết quả
    """
    options = uc.ChromeOptions()
    # options.add_argument("--headless")
    # options.add_argument("--disable-gpu")
    # options.add_argument("--no-sandbox")
    # options.add_argument("--disable-dev-shm-usage")
    
    driver = uc.Chrome(options=options)
    
    try:
        base_url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi"
        all_hrefs = []
        page = 1
        
        while page <= max_pages:
            url = f"{base_url}/p{page}" if page > 1 else base_url
            print(f"Đang xử lý trang {page}/{max_pages}: {url}")
            
            try:
                hrefs = get_hrefs_from_page(driver, url)
                all_hrefs.extend(hrefs)
                print(f"  → Lấy được {len(hrefs)} URLs")
            except Exception as e:
                print(f"  ✗ Lỗi khi crawl trang {page}: {e}")
                break
            
            page += 1
            time.sleep(3)
        
        # Lưu vào file
        if all_hrefs:
            with open(output_file, "w", encoding="utf-8") as file:
                for href in all_hrefs:
                    file.write(href + "\n")
            print(f"\n[SUCCESS] Đã lưu {len(all_hrefs)} URLs vào {output_file}")
        else:
            print("\n[WARNING] Không lấy được URL nào!")
        
        return all_hrefs
        
    finally:
        driver.quit()

def parse_price(price_str):
    """Chuyển đổi giá từ text sang số (VNĐ)"""
    if pd.isna(price_str) or not price_str:
        return np.nan
    
    price_str = price_str.lower().replace(',', '.').strip()
    
    try:
        # Xử lý "Thỏa thuận" hoặc "Liên hệ"
        if 'thỏa thuận' in price_str or 'liên hệ' in price_str:
            return np.nan
        
        # Tách số và đơn vị
        import re
        numbers = re.findall(r'\d+\.?\d*', price_str)
        if not numbers:
            return np.nan
        
        value = float(numbers[0])
        
        # Chuyển đổi theo đơn vị
        if 'tỷ' in price_str or 'ty' in price_str:
            return int(value * 1_000_000_000)
        elif 'triệu' in price_str or 'tr' in price_str:
            return int(value * 1_000_000)
        elif 'nghìn' in price_str or 'ngàn' in price_str:
            return int(value * 1_000)
        else:
            return int(value)
    except:
        return np.nan

def parse_area(area_str):
    """Chuyển đổi diện tích từ text sang số (m²)"""
    if pd.isna(area_str) or not area_str:
        return np.nan
    
    try:
        import re
        numbers = re.findall(r'\d+\.?\d*', str(area_str))
        if numbers:
            return float(numbers[0])
        return np.nan
    except:
        return np.nan

def parse_datetime_to_timestamp(date_str):
    """Chuyển đổi ngày tháng từ 'dd/mm/yyyy' sang timestamp milliseconds"""
    if pd.isna(date_str) or not date_str:
        return np.nan
    
    try:
        from datetime import datetime
        # Parse "15/01/2026" -> timestamp
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        timestamp_ms = int(dt.timestamp() * 1000)
        return timestamp_ms
    except:
        return np.nan

def extract_property_info(driver, url):
    """
    Crawl thông tin chi tiết bất động sản từ URL
    
    Args:
        driver: Selenium WebDriver instance
        url: URL của tin đăng
    """
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "re__pr-specs-content-item-value")))
    
    # Khởi tạo dictionary với các trường theo format yêu cầu
    info = {
        'id': np.nan,
        'title': np.nan,
        'description': np.nan,
        'price': np.nan,
        'area_m2': np.nan,
        'price_per_m2': np.nan,
        'region': np.nan,
        'district': np.nan,
        'ward': np.nan,
        'street': np.nan,
        'lat': np.nan,
        'lng': np.nan,
        'property_type': np.nan,
        'category': np.nan,
        'post_time': np.nan,
        'images': np.nan,
    }
    
    # Xác định category dựa trên URL
    if 'ban-dat' in url:
        info['category'] = 1040
    elif 'ban-can-ho' in url:
        info['category'] = 1010
    elif 'ban-nha' in url or 'ban-shop-house' in url:
        info['category'] = 1020
    
    # Lấy ID từ thuộc tính prid
    try:
        container = driver.find_element(By.CLASS_NAME, "re__pr-container")
        info['id'] = container.get_attribute("prid")
    except:
        pass
    
    # Lấy tiêu đề (title)
    try:
        title_element = driver.find_element(By.CLASS_NAME, "re__pr-title")
        info['title'] = title_element.text.strip()
    except:
        pass
    
    # Lấy mô tả (description)
    try:
        description_element = driver.find_element(By.CLASS_NAME, "re__pr-description")
        info['description'] = description_element.text.strip()
    except:
        pass
    
    # Lấy địa chỉ đầy đủ và phân tách
    try:
        address_element = driver.find_element(By.CLASS_NAME, "re__pr-short-description")
        full_address = address_element.text.strip()
        
        # Phân tách địa chỉ: "Dự án X, Đường Y, Phường Z, Quận/Huyện W, Tỉnh/TP"
        address_parts = [part.strip() for part in full_address.split(',')]
        
        if len(address_parts) >= 5:
            info['street'] = address_parts[1]  # Đường
            info['ward'] = address_parts[2].replace('Phường', '').replace('Xã', '').strip()  # Phường
            info['district'] = address_parts[3].replace('Quận', '').replace('Huyện', '').strip()  # Quận/Huyện
            info['region'] = address_parts[4]  # Tỉnh/Thành phố
        elif len(address_parts) >= 4:
            info['ward'] = address_parts[1].replace('Phường', '').replace('Xã', '').strip()
            info['district'] = address_parts[2].replace('Quận', '').replace('Huyện', '').strip()
            info['region'] = address_parts[3]
        elif len(address_parts) >= 3:
            info['district'] = address_parts[1].replace('Quận', '').replace('Huyện', '').strip()
            info['region'] = address_parts[2]
    except:
        pass
    
    # Lấy giá (price) - giữ dạng text tạm thời
    price_text = np.nan
    try:
        price_element = driver.find_element(By.CSS_SELECTOR, ".re__pr-short-info-item .value")
        price_text = price_element.text.strip()
    except:
        pass
    
    try:
        price_per_m2_elements = driver.find_elements(By.CSS_SELECTOR, ".re__pr-short-info-item .ext")
        if price_per_m2_elements:
            # Bỏ qua price_per_m2 từ web, sẽ tính lại sau
            pass
    except:
        pass
    
    # Lấy diện tích (area_m2) - giữ dạng text tạm thời
    area_text = np.nan
    try:
        specs = driver.find_elements(By.CLASS_NAME, "re__pr-specs-content-item")
        for spec in specs:
            try:
                title = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-title").text.strip()
                value = spec.find_element(By.CLASS_NAME, "re__pr-specs-content-item-value").text.strip()
                
                if title == "Diện tích":
                    area_text = value
                elif title == "Khoảng giá" and pd.isna(price_text):
                    price_text = value
            except:
                pass
    except:
        pass
    
    # Chuyển đổi price và area sang số
    info['price'] = parse_price(price_text)
    info['area_m2'] = parse_area(area_text)
    
    # Tính price_per_m2
    if not pd.isna(info['price']) and not pd.isna(info['area_m2']) and info['area_m2'] > 0:
        info['price_per_m2'] = info['price'] / info['area_m2']
    
    # Lấy tọa độ (lat, lng) và chuyển sang float
    try:
        iframe_element = driver.find_element(By.CSS_SELECTOR, "iframe.lazyload")
        data_src = iframe_element.get_attribute("data-src")
        coords = data_src.split("q=")[1].split(",")
        info['lat'] = float(coords[0].strip())
        info['lng'] = float(coords[1].split("&")[0].strip())
    except:
        pass
    
    # Lấy loại hình bất động sản từ breadcrumb
    try:
        breadcrumbs = driver.find_elements(By.CSS_SELECTOR, ".re__breadcrumb .re__link-se")
        if len(breadcrumbs) > 4:
            info['property_type'] = breadcrumbs[4].text.strip()  # Loại BDS cụ thể
    except:
        pass

    # Lấy ngày đăng (post_time) và chuyển sang timestamp
    try:
        date_element = driver.find_element(By.XPATH, "//div[contains(@class, 're__pr-short-info-item')]//span[@class='title' and text()='Ngày đăng']/following-sibling::span[@class='value']")
        date_str = date_element.text.strip()
        info['post_time'] = parse_datetime_to_timestamp(date_str)
    except:
        pass
    
    # Đếm số lượng ảnh (images)
    try:
        image_elements = driver.find_elements(By.CSS_SELECTOR, ".re__media-preview .swiper-slide")
        info['images'] = len(image_elements)
    except:
        pass
    
    return info

class BatDongSanProducer:
    """Producer gửi dữ liệu bất động sản vào Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='batdongsan-listings'):
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
            value: Dict chứa dữ liệu bất động sản
        """
        try:
            # Chuyển đổi numpy types sang Python native types
            clean_value = {}
            for k, v in value.items():
                if isinstance(v, (np.int64, np.int32)):
                    clean_value[k] = int(v)
                elif isinstance(v, (np.float64, np.float32)):
                    if np.isnan(v):
                        clean_value[k] = None
                    else:
                        clean_value[k] = float(v)
                elif pd.isna(v):
                    clean_value[k] = None
                else:
                    clean_value[k] = v
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=clean_value
            )
            
            # Đợi confirm từ Kafka
            record_metadata = future.get(timeout=10)
            
            print(f"[SUCCESS] Sent ID: {key} → Topic: {record_metadata.topic}, "
                  f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            print(f"[ERROR] Failed to send ID {key}: {e}")
            return False
    
    def close(self):
        """Đóng producer và flush data"""
        self.producer.flush()
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
        print(f"[✗] Kafka connection failed: {e}")
        print("\nHướng dẫn:")
        print("1. Đảm bảo Kafka đang chạy trên WSL2")
        print("2. Kiểm tra port 9092 đã mở")
        print("3. Trong WSL2 chạy: kafka-topics.sh --list --bootstrap-server localhost:9092")
        return False

def create_driver():
    """Tạo Chrome driver với cấu hình tối ưu"""
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.page_load_strategy = 'normal'
    
    try:
        driver = uc.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"[ERROR] Cannot create Chrome driver: {e}")
        raise


if __name__ == "__main__":
    # Đọc cấu hình từ .env file
    WSL2_IP = os.getenv('WSL2_IP', 'localhost')
    KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'batdongsan-listings')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    CRAWL_MODE = os.getenv('CRAWL_MODE', 'use_existing')  # 'crawl_new' hoặc 'use_existing'
    MAX_PAGES = int(os.getenv('MAX_PAGES', '1'))
    
    bootstrap_servers = f'{WSL2_IP}:{KAFKA_PORT}'
    
    print("=" * 60)
    print("BATDONGSAN.COM.VN → KAFKA PRODUCER")
    print("=" * 60)
    print(f"[CONFIG] Kafka Bootstrap: {bootstrap_servers}")
    print(f"[CONFIG] Topic: {KAFKA_TOPIC}")
    print(f"[CONFIG] Batch Size: {BATCH_SIZE}")
    print(f"[CONFIG] Crawl Mode: {CRAWL_MODE}")
    
    # Bước 1: Lấy danh sách URLs
    hrefs_file = "hrefs.txt"
    
    if CRAWL_MODE == 'crawl_new':
        print(f"\n[STEP 1] Crawling URLs from batdongsan.com.vn (max {MAX_PAGES} pages)...")
        try:
            crawl_hrefs_batdongsan(max_pages=MAX_PAGES, output_file=hrefs_file)
        except Exception as e:
            print(f"[ERROR] Failed to crawl URLs: {e}")
            exit(1)
    else:
        print(f"\n[STEP 1] Using existing URLs from {hrefs_file}")
        if not os.path.exists(hrefs_file):
            print(f"[ERROR] File {hrefs_file} not found!")
            print(f"[TIP] Set CRAWL_MODE='crawl_new' in .env to crawl URLs first")
            exit(1)
    
    # Đọc danh sách URLs
    try:
        with open(hrefs_file, "r", encoding="utf-8") as file:
            urls = file.read().splitlines()
        print(f"[INFO] Loaded {len(urls)} URLs from {hrefs_file}")
    except Exception as e:
        print(f"[ERROR] Cannot read {hrefs_file}: {e}")
        exit(1)
    
    if not urls:
        print("[ERROR] No URLs to process!")
        exit(1)
    
    # Bước 2: Test kết nối Kafka
    print(f"\n[STEP 2] Testing Kafka connection...")
    if not test_connection(bootstrap_servers=bootstrap_servers):
        print("\n[TIP] Cập nhật WSL2_IP trong file .env")
        print("[TIP] Lấy IP bằng lệnh: hostname -I (trong WSL2)")
        exit(1)
    
    # Bước 3: Khởi tạo Kafka producer
    print(f"\n[STEP 3] Initializing Kafka producer...")
    kafka_producer = BatDongSanProducer(
        bootstrap_servers=bootstrap_servers,
        topic=KAFKA_TOPIC
    )
    
    # Bước 4: Crawl chi tiết và gửi vào Kafka
    print(f"\n[STEP 4] Crawling details and streaming to Kafka...")
    print("=" * 60)
    
    driver = None
    
    try:
        driver = create_driver()
        csv_filename = "data_bds.csv"
        total_urls = len(urls)
        success_count = 0

        for index, url in enumerate(urls, start=1):
            try:
                print(f"\n[{index}/{total_urls}] Đang xử lý: {url}")
                property_info = extract_property_info(driver, url)
                
                # Thêm metadata
                property_info['crawl_timestamp'] = int(time.time() * 1000)
                property_info['source'] = 'batdongsan.com.vn'
                
                # Gửi vào Kafka
                property_id = property_info.get('id', f'bds_{index}')
                if kafka_producer.send_message(key=property_id, value=property_info):
                    success_count += 1
                
                # Lưu vào CSV
                df = pd.DataFrame([property_info])
                if index == 1:
                    df.to_csv(csv_filename, mode='w', header=True, index=False, encoding='utf-8-sig')
                else:
                    df.to_csv(csv_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
                
                print(f"[INFO] Đã lưu vào CSV và Kafka ({success_count}/{index} success)")
                
                # Flush sau mỗi batch
                if index % BATCH_SIZE == 0:
                    kafka_producer.producer.flush()
                    print(f"[INFO] Flushed batch {index//BATCH_SIZE}")
                
                time.sleep(1)  
                
            except Exception as e:
                print(f"[ERROR] Lỗi khi xử lý link {index}/{total_urls} - {url}: {str(e)}")
                print("[INFO] Restarting driver...")
                
                # Đóng driver cũ an toàn
                try:
                    if driver:
                        driver.quit()
                except:
                    pass
                
                # Tạo driver mới
                try:
                    time.sleep(2)
                    driver = create_driver()
                except Exception as restart_error:
                    print(f"[ERROR] Cannot restart driver: {restart_error}")
                    break

        print(f"\n[COMPLETED] Crawled {total_urls} URLs, sent {success_count} to Kafka")
        
    finally:
        # Đóng driver an toàn và suppress cleanup warnings
        if driver:
            try:
                driver.quit()
                # Set to None để tránh garbage collector gọi __del__ lại
                driver = None
                print("[INFO] Driver closed successfully")
            except Exception as e:
                print(f"[WARNING] Error closing driver: {e}")
        
        # Đóng Kafka producer
        kafka_producer.close()

