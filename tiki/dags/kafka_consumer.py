

from confluent_kafka import Consumer, KafkaError
import psycopg2
import json
import logging
import os

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
logger = init_log("kafka_consumer", "./logs", "kafka_consumer.log")

def insert_batch_to_postgres(cursor, conn, batch):
    """
    Chèn batch dữ liệu vào PostgreSQL.
    """
    try:
        for record in batch:
            cursor.execute("""
                INSERT INTO products (
                    id, sku, name, price, list_price, discount, 
                    discount_rate, rating_average, review_count, 
                    order_count, favourite_count, thumbnail_url, 
                    quantity_sold, original_price, seller_id, seller, seller_product_id,brand_name,category_l1_name,category_l2_name,category_l3_name
                ) VALUES (
                    %(id)s, %(sku)s, %(name)s, %(price)s, %(list_price)s, %(discount)s, 
                    %(discount_rate)s, %(rating_average)s, %(review_count)s, 
                    %(order_count)s, %(favourite_count)s, %(thumbnail_url)s, 
                    %(quantity_sold)s, %(original_price)s, %(seller_id)s, %(seller)s, %(seller_product_id)s,%(brand_name)s,%(category_l1_name)s,%(category_l2_name)s,%(category_l3_name)s

                )
                ON CONFLICT (id) DO NOTHING
            """, record)
        conn.commit()
        logger.info(f"Đã chèn thành công {len(batch)} bản ghi vào PostgreSQL.")
    except Exception as e:
        logger.error(f"Lỗi khi chèn dữ liệu vào PostgreSQL: {e}")
        conn.rollback()  # Rollback nếu có lỗi

def consume_from_kafka(batch_size=100):
    """
    Tiêu thụ dữ liệu từ Kafka và lưu vào PostgreSQL.
    """
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'tiki_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['tiki_products'])

    try:
        conn = psycopg2.connect(
            host="my_postgres",
            database="tiki",
            user="tiki",
            password="tiki"
        )
        cursor = conn.cursor()
        logger.info("Kết nối PostgreSQL thành công.")

        batch = []
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition: {msg.topic()} [{msg.partition()}]")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                batch.append(data)
                logger.info(f"Nhận dữ liệu từ Kafka: {data}")

                if len(batch) >= batch_size:
                    insert_batch_to_postgres(cursor, conn, batch)
                    batch = []  # Xóa batch sau khi chèn
            except json.JSONDecodeError as e:
                logger.error(f"Lỗi JSON không hợp lệ: {msg.value()}, error: {e}")
            except Exception as e:
                logger.error(f"Lỗi không xác định khi xử lý tin nhắn Kafka: {e}")

    except Exception as e:
        logger.error(f"Lỗi khi kết nối hoặc xử lý dữ liệu từ PostgreSQL: {e}")
    finally:
        if batch:
            insert_batch_to_postgres(cursor, conn, batch)  # Chèn dữ liệu còn lại
        cursor.close()
        conn.close()
        consumer.close()
        logger.info("Đã đóng kết nối PostgreSQL và Kafka Consumer.")

# Dành cho test trực tiếp
if __name__ == "__main__":
    consume_from_kafka()
