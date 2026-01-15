# 📅 Kế hoạch bổ sung thống kê theo tháng

## ✅ Đã hoàn thành

### 1. Kiểm tra dữ liệu MongoDB
- ✅ Tạo script `check_mongodb_data.py` để kiểm tra cấu trúc dữ liệu
- ✅ Xác định các field timestamp có sẵn: `processing_time`, `crawl_timestamp`

### 2. Cập nhật Dashboard Fields
- ✅ Thêm `processing_time` và `crawl_timestamp` vào `DASHBOARD_FIELDS`
- ✅ Đảm bảo timestamp được query từ MongoDB

### 3. Tạo hàm tính toán thống kê theo tháng
- ✅ Hàm `get_monthly_stats()`:
  - Sử dụng MongoDB aggregation pipeline
  - Group dữ liệu theo tháng (YYYY-MM)
  - Tính toán: count, avg_price, avg_area, num_districts
  - Tự động detect field timestamp (processing_time hoặc crawl_timestamp)
  - Convert timestamp sang datetime nếu cần

### 4. Thêm UI Components
- ✅ **Metrics Cards so sánh tháng:**
  - Thay đổi số tin đăng (vs tháng trước)
  - Thay đổi giá trung bình (vs tháng trước)
  - Số tháng có dữ liệu
  - Thay đổi diện tích trung bình (vs tháng trước)

- ✅ **Biểu đồ xu hướng:**
  - 📈 Xu hướng Giá trung bình theo tháng (Line chart)
  - 📊 Xu hướng Số lượng tin đăng theo tháng (Bar chart)
  - 📐 Xu hướng Diện tích trung bình theo tháng (Line chart)
  - 💰 Xu hướng Giá/m² trung bình theo tháng (Line chart)

### 5. Cập nhật Callback
- ✅ Thêm 11 outputs mới vào callback `update_dashboard()`
- ✅ Logic tính toán so sánh tháng hiện tại vs tháng trước
- ✅ Tạo các biểu đồ xu hướng với Plotly

## 📋 Cấu trúc dữ liệu MongoDB

### Fields timestamp có sẵn:
1. **`processing_time`** (từ Spark Streaming)
   - Được thêm bởi `current_timestamp()` trong Spark
   - Format: Timestamp khi Spark xử lý data

2. **`crawl_timestamp`** (từ data gốc)
   - Timestamp khi crawl data từ nhatot.com
   - Format: Unix timestamp (seconds hoặc milliseconds)

### Pipeline Aggregation:
```python
pipeline = [
    {"$match": {timestamp_field: {"$exists": True}}},
    {"$project": {
        "year_month": {"$dateToString": {"format": "%Y-%m", "date": ...}},
        "price": 1, "area_m2": 1, "district": 1
    }},
    {"$group": {
        "_id": "$year_month",
        "count": {"$sum": 1},
        "avg_price": {"$avg": "$price"},
        "avg_area": {"$avg": "$area_m2"},
        "districts": {"$addToSet": "$district"}
    }},
    {"$sort": {"_id": 1}}
]
```

## 🎯 Tính năng đã thêm

### 1. Metrics Cards so sánh
- **Thay đổi số tin đăng:**
  - Hiển thị: `+123` hoặc `-45`
  - Mô tả: "vs tháng trước (01/2025) (+10.5%)"

- **Thay đổi giá trung bình:**
  - Hiển thị: `+0.5 tỷ` hoặc `-200 triệu`
  - Mô tả: "vs tháng trước (01/2025) (+5.2%)"

- **Số tháng có dữ liệu:**
  - Hiển thị tổng số tháng đã thu thập

- **Thay đổi diện tích trung bình:**
  - Hiển thị: `+5.2 m²` hoặc `-3.1 m²`
  - Mô tả: "vs tháng trước (01/2025) (+2.1%)"

### 2. Biểu đồ xu hướng

#### a) Xu hướng Giá trung bình
- **Type:** Line chart với markers
- **X-axis:** Tháng (MM/YYYY)
- **Y-axis:** Giá trung bình (Tỷ VNĐ)
- **Color:** Gradient purple (#667eea → #764ba2)

#### b) Xu hướng Số lượng tin đăng
- **Type:** Bar chart với color scale
- **X-axis:** Tháng (MM/YYYY)
- **Y-axis:** Số lượng tin đăng
- **Color:** Blues color scale

#### c) Xu hướng Diện tích trung bình
- **Type:** Line chart với markers
- **X-axis:** Tháng (MM/YYYY)
- **Y-axis:** Diện tích trung bình (m²)
- **Color:** Gradient pink (#f093fb → #f5576c)

#### d) Xu hướng Giá/m² trung bình
- **Type:** Line chart với markers
- **X-axis:** Tháng (MM/YYYY)
- **Y-axis:** Giá/m² trung bình (Triệu VNĐ)
- **Color:** Gradient blue (#4facfe → #00f2fe)

## 🔧 Cách sử dụng

### 1. Kiểm tra dữ liệu MongoDB
```bash
python check_mongodb_data.py
```

Script này sẽ:
- Kiểm tra kết nối MongoDB
- Liệt kê các field timestamp có sẵn
- Hiển thị thống kê theo tháng (nếu có)
- Đề xuất các cải tiến

### 2. Chạy Dashboard
```bash
python dashboard.py
```

Dashboard sẽ tự động:
- Load dữ liệu từ MongoDB
- Tính toán thống kê theo tháng
- Hiển thị metrics cards so sánh
- Vẽ các biểu đồ xu hướng
- Auto-refresh mỗi 30 giây

### 3. Xem kết quả
Truy cập: `http://localhost:8050`

Scroll xuống phần **"📅 Thống kê theo tháng"** để xem:
- Metrics cards so sánh
- 4 biểu đồ xu hướng

## ⚠️ Lưu ý

### 1. Yêu cầu dữ liệu
- **Tối thiểu:** Cần ít nhất 2 tháng dữ liệu để so sánh
- **Nếu chỉ có 1 tháng:** Metrics cards sẽ hiển thị "N/A" với mô tả "Chưa đủ dữ liệu"

### 2. Field timestamp
- Dashboard tự động detect field timestamp có sẵn
- Ưu tiên: `processing_time` > `crawl_timestamp`
- Nếu không có timestamp: Biểu đồ sẽ hiển thị "Chưa có dữ liệu theo tháng"

### 3. Performance
- Thống kê theo tháng được tính toán mỗi lần refresh (30s)
- Sử dụng MongoDB aggregation pipeline (nhanh hơn so với query toàn bộ data)
- Có thể cache nếu cần tối ưu thêm

## 🚀 Cải tiến tương lai (nếu cần)

1. **Cache monthly stats:**
   - Cache kết quả aggregation trong 5-10 phút
   - Giảm load MongoDB khi có nhiều users

2. **So sánh với nhiều tháng trước:**
   - So sánh với 3 tháng, 6 tháng, 12 tháng trước
   - Thêm dropdown để chọn khoảng thời gian

3. **Phân tích theo quận/huyện theo tháng:**
   - Top quận/huyện mỗi tháng
   - Xu hướng giá theo quận/huyện

4. **Dự đoán xu hướng:**
   - Sử dụng linear regression để dự đoán giá tháng tiếp theo
   - Hiển thị forecast trên biểu đồ

5. **Export dữ liệu:**
   - Export thống kê theo tháng ra CSV/Excel
   - Download biểu đồ dưới dạng PNG/PDF

## 📝 Files đã thay đổi

1. **`dashboard.py`**
   - Thêm `get_monthly_stats()` function
   - Cập nhật `DASHBOARD_FIELDS` (thêm timestamp fields)
   - Thêm UI components cho monthly stats
   - Cập nhật callback với 11 outputs mới
   - Thêm logic tính toán so sánh tháng

2. **`check_mongodb_data.py`** (mới)
   - Script kiểm tra cấu trúc dữ liệu MongoDB
   - Phân tích các field timestamp
   - Hiển thị thống kê theo tháng

3. **`spark_streaming_consumer.py`**
   - Đã có sẵn `processing_time` field (không cần sửa)

## ✅ Checklist hoàn thành

- [x] Kiểm tra dữ liệu MongoDB
- [x] Cập nhật DASHBOARD_FIELDS
- [x] Tạo hàm get_monthly_stats()
- [x] Thêm UI components (metrics cards)
- [x] Thêm biểu đồ xu hướng (4 charts)
- [x] Cập nhật callback với logic tính toán
- [x] Test với dữ liệu thực tế
- [x] Xử lý edge cases (không có dữ liệu, chỉ 1 tháng)

## 🎉 Kết quả

Dashboard hiện có đầy đủ tính năng thống kê theo tháng:
- ✅ So sánh tháng hiện tại vs tháng trước
- ✅ 4 biểu đồ xu hướng đẹp mắt
- ✅ Tự động detect timestamp field
- ✅ Xử lý edge cases tốt
- ✅ UI/UX nhất quán với phần còn lại của dashboard
