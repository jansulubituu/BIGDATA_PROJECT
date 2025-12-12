
import os
import json
import datetime
import requests
import csv
import time


def fetch_house_ids(limit_ids=300, region=12000, category=1000):
    """Lấy list_id từ API listing Nhà Tốt."""
    ids = set()
    per_page = 20

    for page in range(0, limit_ids // per_page + 2):
        offset = page * per_page
        url = (
            f"https://gateway.chotot.com/v1/public/ad-listing?"
            f"region_v2={region}&cg={category}&o={offset}&limit={per_page}"
        )
        headers = {"User-Agent": "Mozilla/5.0"}

        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print("[WARN] Lỗi request:", resp.text)
            break

        ads = resp.json().get("ads", [])
        if not ads:
            break

        for ad in ads:
            if "list_id" in ad:
                ids.add(ad["list_id"])

        if len(ids) >= limit_ids:
            break

        time.sleep(0.3)

    print(f"[INFO] Lấy được {len(ids)} list_id.")
    return list(ids)


def fetch_house_detail(ad_id):
    """API chi tiết 1 tin BĐS."""
    url = f"https://gateway.chotot.com/v1/public/ad-listing/{ad_id}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except:
        return None


def extract_one(raw):
    """Chuẩn hóa dữ liệu từ JSON → dict CSV."""
    if raw is None or "ad" not in raw:
        return None

    ad = raw["ad"]

    price = ad.get("price")
    area = ad.get("area")
    ppm2 = price / area if (price and area and area > 0) else None

    return {
        "id": ad.get("list_id"),
        "title": ad.get("subject"),
        "description": ad.get("body"),
        "price": price,
        "area_m2": area,
        "price_per_m2": ppm2,
        "region": ad.get("region_name"),
        "district": ad.get("area_name"),
        "ward": ad.get("ward_name"),
        "street": ad.get("street_name"),
        "lat": ad.get("latitude"),
        "lng": ad.get("longitude"),
        "property_type": ad.get("property_type"),
        "category": ad.get("category"),
        "post_time": ad.get("list_time"),
        "images": len(ad.get("images", [])),
    }


def save_to_csv(filename, rows):
    """Lưu danh sách dict vào CSV."""
    if not rows:
        print("[WARN] Không có dữ liệu để lưu CSV.")
        return

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def extract_house(limit_rows=300):
    """
    Hàm duy nhất gọi trong DAG Airflow:
    - Crawl list ID
    - Crawl chi tiết từng ID
    - Lưu JSON từng record
    - Lưu CSV tổng hợp
    """
    today = datetime.date.today().isoformat()
    DOWNLOAD_DIR = f"data_input/house/{today}"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    timestamp = datetime.datetime.utcnow().isoformat()
    rows = []

    # 1) Crawl danh sách ID
    ids = fetch_house_ids(limit_ids=limit_rows)

    # 2) Crawl từng tin
    for ad_id in ids:
        raw = fetch_house_detail(ad_id)
        clean = extract_one(raw)

        if clean:
            rows.append(clean)

            out_json = os.path.join(
                DOWNLOAD_DIR,
                f"house_{ad_id}_{timestamp.replace(':','')}.json"
            )
            with open(out_json, "w", encoding="utf-8") as f:
                json.dump(clean, f, ensure_ascii=False, indent=2)

        time.sleep(0.2)

    # 3) Lưu CSV tổng
    out_csv = os.path.join(
        DOWNLOAD_DIR,
        f"house_data_{timestamp.replace(':','')}.csv"
    )
    save_to_csv(out_csv, rows)

    print(f"[INFO] Extracted {len(rows)} listings → CSV: {out_csv}")
    return os.path.abspath(out_csv)


if __name__ == "__main__":
    path = extract_house(limit_rows=300)
    print("CSV Saved:", path)