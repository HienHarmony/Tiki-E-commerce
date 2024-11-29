
from confluent_kafka import Producer
import os
import csv
import json
import logging

def init_log(logger_name, log_dir, log_file):
    """
    Khởi tạo logger để ghi log.
    """
    try:
        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        log_path = os.path.join(log_dir, log_file)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        return logger
    except Exception as e:
        raise RuntimeError(f"Lỗi khi khởi tạo logger: {e}")

# Initialize logger
logger = init_log("kafka_producer", "./logs", "kafka_producer.log")

def produce_to_kafka(input_dir="/opt/airflow/dags/processed_data", kafka_server="kafka:9092", topic="tiki_products"):
    """
    Gửi dữ liệu từ các file CSV đã xử lý lên Kafka.
    """
    try:
        kafka_config = {'bootstrap.servers': kafka_server}
        producer = Producer(kafka_config)

        if not os.path.exists(input_dir):
            logger.error(f"Thư mục {input_dir} không tồn tại.")
            return

        files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
        if not files:
            logger.warning(f"Không có file CSV nào trong thư mục {input_dir}.")
            return

        for file_name in files:
            file_path = os.path.join(input_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    producer.produce(
                        topic,
                        value=json.dumps(row),
                        callback=lambda err, msg: logger.error(f"Lỗi: {err}") if err else logger.info(f"Đã gửi tới {msg.topic()} offset {msg.offset()}")
                    )
                    producer.poll(0)

        producer.flush()
        logger.info("Gửi dữ liệu lên Kafka thành công.")
    except Exception as e:
        logger.error(f"Lỗi khi gửi dữ liệu lên Kafka: {e}")
        raise
