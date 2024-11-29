Hệ Thống Pipeline Dữ Liệu Tiki với Airflow, Kafka và PostgreSQL
Dự án này triển khai một hệ thống pipeline dữ liệu tự động để cào dữ liệu từ Tiki.vn, xử lý thông qua Apache Kafka và lưu trữ vào PostgreSQL, sử dụng Airflow để điều phối.

tiki/
├── dags/                      # Airflow DAGs
│   ├── workflow.py            # Định nghĩa DAG chính
│   ├── log.py                 # Cấu hình logging
│   └── result.py    
│   ├── kafka_consumer.py      # Script Kafka Consumer (đọc dữ liệu từ Kafka) và insert vào postgresql
│   ├── kafka_producer.py   # Script Kafka Producer (gửi dữ liệu vào Kafka)
|   |__ process_data.py         # Script xử lý dữ liệu
|   |__ data                 # thư mục chứa data sau khi crawl về
|   |__ logs                # thư mục chứa log
|   |__ processed_data      # thư mục chứa dữ liệu đã được xử lý    
├── scripts/                   # Các script hỗ trợ
│   ├── init_postgres.sql      # Script SQL tạo bảng PostgreSQL
    # Script Kafka Producer (gửi dữ liệu vào Kafka)
├── logs/                      # Thư mục chứa file log
├── docker-compose.yaml        # Cấu hình Docker Compose
├── Dockerfile                 # Cấu hình Dockerfile cho Airflow
├── requirements.txt           # Danh sách thư viện cần thiết
|└── README.md                  # Hướng dẫn triển khai và sử dụng
|___.env
|___.dockerignore                
# RUN Docker build
docker-compose up --build
3. Truy cập Airflow
http://localhost:8085
4. Chạy DAG
5. Kiểm tra kết quả
6. Dừng Docker Compose
bash -c "docker-compose down"
7 . truy cập kafka -ui
http://localhost:8080

check topic kafka :
docker exec -it kafka bash 
kafka-topics --bootstrap-server localhost:9092 --list
check consumer :
kafka-console-consumer --bootstrap-server kafka:9092 --topic tiki_products --from-beginning
check consumer group :
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group tiki_consumer_group
check postgresql :
bước 1 : docker exec -it tiki_postgres bash
bước 2 : psql -U tiki -d tiki
bước 3 truy vấn xem có dữ liệu : SELECT * FROM products LIMIT 10;




''' dấu hiệu nhận biết airflow khởi động được '''
docker logs tiki-airflow-webserver-1
[2024-11-24 01:53:30 +0000] [23] [INFO] Listening at: http://0.0.0.0:8080 (23)
[2024-11-24 01:53:30 +0000] [23] [INFO] Using worker: sync
[2024-11-24 01:53:30 +0000] [64] [INFO] Booting worker with pid: 64
[2024-11-24 01:53:30 +0000] [65] [INFO] Booting worker with pid: 65
[2024-11-24 01:53:30 +0000] [66] [INFO] Booting worker with pid: 66
[2024-11-24 01:53:31 +0000] [67] [INFO] Booting worker with pid: 67

connect powerBi
Server: localhost 
Database: tiki
Username: tiki
Password: tiki
port : 1705