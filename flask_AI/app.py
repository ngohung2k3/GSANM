import os
import time
import json
import re
import pickle
import urllib.parse
import logging
import datetime
from typing import Optional

import requests
from kafka import KafkaConsumer

# -------------------------
# Config & Paths
# -------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "rf_model.pkl")
VECTORIZER_PATH = os.path.join(BASE_DIR, "vectorizer_rf.pkl")

LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100/loki/api/v1/push")
LOKI_JOB = os.environ.get("LOKI_JOB", "flask-ai")
LOKI_APP = os.environ.get("LOKI_APP", "flask-ai")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "web-logs")

LABEL_MAP = {0: "norm", 1: "sqli", 2: "xss"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("flask-ai")

# -------------------------
# Load model & vectorizer
# -------------------------
model: Optional[object] = None
vectorizer: Optional[object] = None

def load_artifacts():
    global model, vectorizer
    try:
        with open(VECTORIZER_PATH, "rb") as vf:
            vectorizer = pickle.load(vf)
        logger.info("Vectorizer loaded.")
    except Exception as e:
        logger.error(f"Error loading vectorizer: {e}")
    try:
        with open(MODEL_PATH, "rb") as mf:
            model = pickle.load(mf)
        logger.info("Model loaded.")
    except Exception as e:
        logger.error(f"Error loading model: {e}")

load_artifacts()

# -------------------------
# Utilities
# -------------------------
def decode_url(encoded: str) -> str:
    try:
        decoded = urllib.parse.unquote_plus(str(encoded))
    except Exception:
        decoded = str(encoded)
    decoded = decoded.lower().strip()
    decoded = re.sub(r"\s+", " ", decoded)
    return decoded

def extract_query_string(log_line: str) -> str:
    try:
        # Lấy phần sau dấu ?
        match = re.search(r'\?(.*?)(?: HTTP/\d\.\d"|")', log_line)
        if match:
            query = match.group(1)
            return urllib.parse.unquote_plus(query)
        return ""
    except:
        return ""

def predict_text(raw_text: str) -> str:
    if model is None or vectorizer is None:
        return "error"

    query = extract_query_string(raw_text)
    if not query:
        return "norm"

    processed = decode_url(query) 
    X = vectorizer.transform([processed])
    pred = model.predict(X)[0]
    return LABEL_MAP.get(pred, "unknown")
    

# ⭐ CHỈNH SỬA: Thêm tham số `timestamp_ns`
def push_to_loki(raw_text: str, prediction_label: str, timestamp_ns: str) -> None:
    try:
        # Sử dụng timestamp được truyền vào (Kafka Timestamp)
        labels = {
            "job": LOKI_JOB,
            "app": LOKI_APP,
            "type": prediction_label
        }
        payload = {
            "streams": [
                {
                    "stream": labels,
                    "values": [
                        # ⭐ CHỈNH SỬA: Sử dụng timestamp_ns đã nhận
                        [timestamp_ns, json.dumps({
                            "input": raw_text,
                            "prediction": prediction_label
                        })]
                    ]
                }
            ]
        }
        r = requests.post(LOKI_URL, json=payload, timeout=3)
        if r.status_code not in (200, 204):
            logger.warning(f"Loki push failed ({r.status_code}): {r.text}")
    except Exception as e:
        logger.warning(f"Loki connection error: {e}")

# -------------------------
# Kafka Consumer
# -------------------------
def start_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # ⭐ CHỈNH SỬA: Đặt thành 'earliest' để đọc lại các logs cũ sau khi khởi động lại
        auto_offset_reset='earliest', 
        enable_auto_commit=True,
        group_id='flask-ai-consumer',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    logger.info(f"Listening to Kafka topic '{KAFKA_TOPIC}'...")
    for message in consumer:
        raw_text = message.value.strip()
        if not raw_text:
            continue
            
        # ⭐ CHỈNH SỬA: Trích xuất Kafka Timestamp (mili-giây) và chuyển sang nano-giây
        # message.timestamp là thời gian Kafka nhận message (tính bằng mili-giây)
        kafka_timestamp_ms = message.timestamp
        if kafka_timestamp_ms is None:
            # Fallback nếu Kafka timestamp không có, sử dụng thời gian hiện tại
            logger.warning("Kafka timestamp missing, using current time.")
            timestamp_ns = str(int(time.time() * 1e9))
        else:
            # Chuyển từ mili-giây sang nano-giây (1e6 = 1,000,000)
            timestamp_ns = str(kafka_timestamp_ms * 1_000_000)
            
        label = predict_text(raw_text)
        
        # ⭐ CHỈNH SỬA: Truyền Kafka Timestamp vào hàm đẩy log
        push_to_loki(raw_text, label, timestamp_ns)
        
        logger.info(f"KAFKA {label.upper()} → {raw_text[:100]}")

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    # Đảm bảo Kafka đã sẵn sàng
    time.sleep(15) 
    start_kafka_consumer()
