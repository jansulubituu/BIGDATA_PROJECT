from pymongo import MongoClient
import json
from dotenv import load_dotenv
import os
load_dotenv()

# Kết nối
client = MongoClient(os.getenv("MONGODB_URI"))
db = client['bigdata_houses']
collection = db['listings']

# Lấy dữ liệu
data = list(collection.find())

# Xuất ra file JSON
with open('data_output/houses.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2, default=str)