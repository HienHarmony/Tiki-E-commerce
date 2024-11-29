

import pandas as pd
import os
import logging
import ast

# Khởi tạo logger
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
logger = init_log("process_data", "./logs", "process_data.log")

def clean_quantity_sold(dataframe):
    """
    Xử lý cột `quantity_sold` từ chuỗi JSON thành giá trị số.
    """
    try:
        dataframe['quantity_sold'] = dataframe['quantity_sold'].apply(
            lambda x: ast.literal_eval(x)['value'] if pd.notnull(x) and isinstance(x, str) else 0
        )
        logger.info("Xử lý cột 'quantity_sold' thành công.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý cột 'quantity_sold': {e}")
        raise

def process_all_files(input_dir, output_dir):
    """
    Xử lý tất cả file CSV trong thư mục đầu vào và lưu kết quả vào thư mục đầu ra.
    """
    try:
        if not os.path.exists(input_dir):
            logger.error(f"Thư mục đầu vào {input_dir} không tồn tại.")
            return

        os.makedirs(output_dir, exist_ok=True)
        csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]

        if not csv_files:
            logger.warning(f"Không có file CSV nào trong thư mục {input_dir}.")
            return

        for file_name in csv_files:
            input_file = os.path.join(input_dir, file_name)
            output_file = os.path.join(output_dir, file_name)

            logger.info(f"Đang xử lý file {input_file}.")
            df = pd.read_csv(input_file)

            # Xử lý cột `quantity_sold`
            if 'quantity_sold' in df.columns:
                clean_quantity_sold(df)
            else:
                logger.warning(f"Cột 'quantity_sold' không tồn tại trong file {input_file}.")

            # Lưu file đã xử lý
            df.to_csv(output_file, index=False)
            logger.info(f"Dữ liệu đã được xử lý và lưu vào {output_file}.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý dữ liệu: {e}")
        raise
