import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# -------------------------
# Cấu hình
# -------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "web-logs")
LOG_PATH = "/var/log/apache2/access.log"

# ⭐ FILE DÙNG ĐỂ LƯU VỊ TRÍ ĐÃ ĐỌC CUỐI CÙNG
POSITION_FILE = os.getenv("POSITION_FILE", "/app/producer_offset.txt") 

# -------------------------
# Kafka Producer Setup (Giữ nguyên)
# -------------------------
def init_producer():
    max_retries = 10
    retry_delay = 5  # giây
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: v.encode("utf-8")
            )
            print(f"Kafka producer connected successfully on attempt {attempt+1}")
            return producer
        except (NoBrokersAvailable, KafkaError) as e:
            print(f"Kafka connection failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    raise Exception("Failed to connect to Kafka after max retries.")

producer = init_producer()

# -------------------------
# Logic Tailing Mới
# -------------------------

def load_position():
    """Tải vị trí byte offset cuối cùng đã lưu."""
    if os.path.exists(POSITION_FILE):
        with open(POSITION_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

def save_position(position):
    """Lưu vị trí byte offset hiện tại."""
    with open(POSITION_FILE, "w") as f:
        f.write(str(position))

def stream_log_with_offset():
    # 1. Tải vị trí đã đọc cuối cùng
    current_position = load_position()
    print(f"Starting log stream from byte offset: {current_position}")
    
    # Mở file chỉ một lần
    with open(LOG_PATH, "r") as f:
        # Di chuyển đến vị trí đã lưu
        f.seek(current_position)
        
        while True:
            line = f.readline()
            if not line:
                # Nếu không có dòng mới, chờ và thử lại (mô phỏng tail -f)
                time.sleep(1)
                continue
            
            # Xử lý dòng log mới
            line = line.strip()
            if line:
                try:
                    # Gửi lên Kafka
                    producer.send(KAFKA_TOPIC, value=line).get(timeout=10)
                    print(f"Sent: {line[:100]} | New position: {f.tell()}")
                    
                    # Cập nhật và lưu vị trí mới sau khi gửi thành công
                    current_position = f.tell()
                    save_position(current_position)
                    
                except KafkaError as e:
                    print(f"Kafka send error: {e}")
                except Exception as e:
                    print(f"General error: {e}")


if __name__ == "__main__":
    stream_log_with_offset()
