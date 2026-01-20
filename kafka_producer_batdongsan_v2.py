"""
Kafka Producer - Gửi dữ liệu bất động sản từ batdongsan.com.vn vào Kafka topic
Tối ưu tốc độ với Playwright
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import time
import numpy as np
import pandas as pd
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
import re
from datetime import datetime

# Load environment variables từ file .env
load_dotenv()


def get_hrefs_from_page(page, url, base_domain="https://batdongsan.com.vn"):
    """Lấy danh sách href từ một trang với Playwright"""
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_selector(".js__product-link-for-product-id", timeout=10000)
        
        elements = page.query_selector_all(".js__product-link-for-product-id")
        hrefs = []
        for element in elements:
            href = element.get_attribute("href")
            # Chuyển relative URL thành absolute URL
            if href and not href.startswith(('http://', 'https://')):
                href = base_domain + href
            hrefs.append(href)
        return hrefs
    except PlaywrightTimeout:
        print(f"  ✗ Timeout khi load trang")
        return []


def crawl_hrefs_batdongsan(max_pages=1, output_file="hrefs.txt"):
    """
    Crawl danh sách URL bất động sản từ batdongsan.com.vn với Playwright
    
    Args:
        max_pages: Số trang tối đa cần crawl
        output_file: File lưu kết quả
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        page = context.new_page()
        
        # Chặn load ảnh, CSS, fonts để tăng tốc
        page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ico}", lambda route: route.abort())
        
        try:
            base_url = "https://batdongsan.com.vn/nha-dat-cho-thue-ha-noi"
            # base_url = "https://batdongsan.com.vn/nha-dat-ban-ha-noi"
            all_hrefs = []
            # page_num = 412 #nha ban
            page_num = 400  # nhà cho thuê
            end_page = page_num + max_pages - 1
            
            while page_num <= end_page:
                url = f"{base_url}/p{page_num}" if page_num > 1 else base_url
                print(f"Đang xử lý trang {page_num}/{end_page}: {url}")
                
                hrefs = get_hrefs_from_page(page, url)
                if hrefs:
                    all_hrefs.extend(hrefs)
                    print(f"  → Lấy được {len(hrefs)} URLs")
                else:
                    print(f"  ✗ Không lấy được URL nào, dừng crawl")
                    break
                
                page_num += 1
                time.sleep(2)
            
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
            browser.close()


def parse_price(price_str):
    """Chuyển đổi giá từ text sang số (VNĐ)"""
    if pd.isna(price_str) or not price_str:
        return np.nan
    
    price_str_lower = str(price_str).lower().strip()
    
    try:
        if 'thỏa thuận' in price_str_lower or 'liên hệ' in price_str_lower:
            return np.nan
        
        # Xử lý định dạng số Việt Nam: loại bỏ dấu chấm (phân cách nghìn), giữ dấu phẩy (thập phân)
        clean_str = str(price_str)
        # Loại bỏ dấu chấm (phân cách hàng nghìn)
        clean_str = clean_str.replace('.', '')
        # Thay dấu phẩy (thập phân) thành dấu chấm cho float
        clean_str = clean_str.replace(',', '.')
        
        numbers = re.findall(r'\d+\.?\d*', clean_str)
        if not numbers:
            return np.nan
        
        value = float(numbers[0])
        
        if 'tỷ' in price_str_lower or 'ty' in price_str_lower:
            return int(value * 1_000_000_000)
        elif 'triệu' in price_str_lower or 'tr' in price_str_lower:
            return int(value * 1_000_000)
        elif 'nghìn' in price_str_lower or 'ngàn' in price_str_lower:
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
        # Trong tiếng Việt: dấu chấm (.) là phân cách hàng nghìn, dấu phẩy (,) là thập phân
        clean_str = str(area_str)
        
        # Loại bỏ dấu chấm (phân cách hàng nghìn)
        clean_str = clean_str.replace('.', '')
        # Thay dấu phẩy (thập phân) thành dấu chấm cho float
        clean_str = clean_str.replace(',', '.')
        
        # Tìm số
        numbers = re.findall(r'\d+\.?\d*', clean_str)
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
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        timestamp_ms = int(dt.timestamp() * 1000)
        return timestamp_ms
    except:
        return np.nan


def extract_property_info(page, url):
    """
    Crawl thông tin chi tiết bất động sản từ URL với Playwright
    
    Args:
        page: Playwright Page instance
        url: URL của tin đăng
    """
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_selector(".re__pr-specs-content-item-value", timeout=10000)
    except PlaywrightTimeout:
        print(f"  ✗ Timeout khi load trang detail")
        return None
    
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
    elif 'cho-thue-nha-tro' in url or 'cho-thue-can-ho' in url or 'cho-thue-nha' in url:
        info['category'] = 1050
    elif 'cho-thue-van-phong' in url:
        info['category'] = 1030
    else:
        info['category'] = 9999  # Unknown category        
    
    # Lấy ID từ thuộc tính prid hoặc URL
    try:
        container = page.query_selector(".re__pr-container")
        if container:
            prid = container.get_attribute("prid")
            if prid:
                info['id'] = int(prid)  # Chuyển sang số nguyên
    except:
        pass
    
    # Nếu không lấy được từ HTML, extract từ URL
    if pd.isna(info['id']) or not info['id']:
        try:
            import re
            # URL format: .../ten-bds-pr44150507 hoặc .../ten-bds-pr44150507.html
            match = re.search(r'-pr(\d+)', url)
            if match:
                info['id'] = int(match.group(1))  # Chuyển sang số nguyên
        except:
            pass
    
    # Lấy tiêu đề (title)
    try:
        title_element = page.query_selector(".re__pr-title")
        if title_element:
            info['title'] = title_element.inner_text().strip()
    except:
        pass
    
    # Lấy mô tả (description)
    try:
        description_element = page.query_selector(".re__pr-description")
        if description_element:
            info['description'] = description_element.inner_text().strip()
    except:
        pass
    
    # Lấy địa chỉ từ breadcrumb theo level
    try:
        breadcrumb_links = page.query_selector_all(".re__breadcrumb a.re__link-se[level]")
        if breadcrumb_links:
            for link in breadcrumb_links:
                level = link.get_attribute("level")
                text = link.inner_text().strip()
                
                if level == "2":  # Level 2: Thành phố/Tỉnh (vd: Hà Nội)
                    info['region'] = text
                elif level == "3":  # Level 3: Quận/Huyện (vd: Tây Hồ)
                    info['district'] = text
                elif level == "4":  # Level 4: Phường/Xã hoặc Dự án (vd: Heritage West Lake)
                    # Có thể là ward hoặc project name, tùy vào cấu trúc
                    if 'ward' not in info or pd.isna(info['ward']):
                        info['ward'] = text
    except:
        pass
    
    # Lấy thông tin địa chỉ chi tiết từ short description (nếu cần bổ sung)
    try:
        address_element = page.query_selector(".re__pr-short-description")
        if address_element:
            full_address = address_element.inner_text().strip()
            address_parts = [part.strip() for part in full_address.split(',')]
            
            # Lấy street từ phần đầu tiên nếu có
            if len(address_parts) >= 2 and not address_parts[0].startswith('Phường') and not address_parts[0].startswith('Xã'):
                if 'street' not in info or pd.isna(info['street']):
                    info['street'] = address_parts[1] if len(address_parts) > 1 else address_parts[0]
            
            # Bổ sung ward từ address nếu chưa có
            if ('ward' not in info or pd.isna(info['ward'])) and len(address_parts) >= 3:
                for part in address_parts:
                    if 'Phường' in part or 'Xã' in part:
                        info['ward'] = part.replace('Phường', '').replace('Xã', '').strip()
                        break
    except:
        pass
    
    # Lấy giá (price)
    price_text = np.nan
    try:
        price_element = page.query_selector(".re__pr-short-info-item .value")
        if price_element:
            price_text = price_element.inner_text().strip()
    except:
        pass
    
    # Lấy diện tích (area_m2)
    area_text = np.nan
    try:
        specs = page.query_selector_all(".re__pr-specs-content-item")
        for spec in specs:
            try:
                title_el = spec.query_selector(".re__pr-specs-content-item-title")
                value_el = spec.query_selector(".re__pr-specs-content-item-value")
                
                if title_el and value_el:
                    title = title_el.inner_text().strip()
                    value = value_el.inner_text().strip()
                    
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
    
    # Lấy tọa độ (lat, lng)
    try:
        iframe_element = page.query_selector("iframe.lazyload")
        if iframe_element:
            data_src = iframe_element.get_attribute("data-src")
            if data_src and "q=" in data_src:
                coords = data_src.split("q=")[1].split(",")
                info['lat'] = float(coords[0].strip())
                info['lng'] = float(coords[1].split("&")[0].strip())
    except:
        pass
    
    # Lấy loại hình bất động sản từ breadcrumb
    try:
        breadcrumbs = page.query_selector_all(".re__breadcrumb .re__link-se")
        if len(breadcrumbs) > 4:
            info['property_type'] = breadcrumbs[4].inner_text().strip()
    except:
        pass

    # Lấy ngày đăng (post_time)
    try:
        date_element = page.query_selector("xpath=//div[contains(@class, 're__pr-short-info-item')]//span[@class='title' and text()='Ngày đăng']/following-sibling::span[@class='value']")
        if date_element:
            date_str = date_element.inner_text().strip()
            info['post_time'] = parse_datetime_to_timestamp(date_str)
    except:
        pass
    
    # Đếm số lượng ảnh (images)
    try:
        image_elements = page.query_selector_all(".re__media-preview .swiper-slide")
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
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'
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
    print("BATDONGSAN.COM.VN → KAFKA PRODUCER (PLAYWRIGHT)")
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
    
    # Bước 4: Crawl chi tiết và gửi vào Kafka với Playwright
    print(f"\n[STEP 4] Crawling details and streaming to Kafka...")
    print("=" * 60)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        page = context.new_page()
        
        # Chặn load ảnh, CSS, fonts để tăng tốc 3-5x
        page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2,ico}", lambda route: route.abort())
        
        try:
            csv_filename = "data_bds.csv"
            total_urls = len(urls)
            success_count = 0

            for index, url in enumerate(urls, start=1):
                try:
                    print(f"\n[{index}/{total_urls}] Đang xử lý: {url}")
                    property_info = extract_property_info(page, url)
                    
                    if property_info is None:
                        print(f"[WARNING] Bỏ qua URL do timeout/lỗi")
                        continue
                    
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
                    # Playwright tự động recover, không cần restart browser
                    time.sleep(2)
                    continue

            print(f"\n[COMPLETED] Crawled {total_urls} URLs, sent {success_count} to Kafka")
            
        finally:
            browser.close()
            kafka_producer.close()
            print("[INFO] Browser và Kafka producer đã đóng")