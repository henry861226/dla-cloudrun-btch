import logging
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()
# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_current_time(format_str: str) -> str:
    """取得格式化時間"""
    return datetime.now().strftime(format_str)

def check_table_exist(table_id):
    for attempt in range(5):
        try:
            bigquery_client.get_table(f"{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.{table_id}")
            logging.info(f"Table {table_id} exists. Proceeding with Next query.")
            break
        except NotFound:
            logging.info(f"Table {table_id} not found. Retrying in 2 seconds... (Attempt {attempt + 1}/5)")
            time.sleep(2)
    else:
        print("Table creation verification failed. Exiting.")
        return