import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()
# 初始化 GCS 客戶端
storage_client = storage.Client()
# 設置query模板文件目錄
env = Environment(loader=FileSystemLoader('query'))
# 定義台北時區
tz_taipei = ZoneInfo("Asia/Taipei")
# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_current_time(format_str: str) -> str:
    """取得格式化時間"""
    return datetime.now(tz=tz_taipei).strftime(format_str)

def execute_bq_query(template_name, render_params):
    try:
        # 加載模板
        template = env.get_template(template_name)
        # 渲染模板
        query = template.render(render_params)
        # 執行 BigQuery 查詢
        bigquery_client.query(query).result()
        logging.info(f"Query executed successfully for template: {template_name}")
    except Exception as e:
        logging.error(f"Error executing query for template {template_name}: {e}")
        raise

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

def check_gcs_file_ready(bucket_name, file_name, max_retries=5, wait_seconds=3):
    """確認 GCS 檔案是否存在且準備就緒（最多嘗試 max_retries 次）"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    for attempt in range(max_retries):
        if blob.exists():
            logging.info(f"GCS 檔案 {file_name} 已準備就緒（嘗試次數: {attempt + 1}）。")
            return True
        else:
            logging.warning(f"GCS 檔案 {file_name} 尚未準備好，等待 {wait_seconds} 秒後重試...（第 {attempt + 1} 次）")
            time.sleep(wait_seconds)
    
    logging.warning(f"GCS 檔案 {file_name} 在嘗試 {max_retries} 次後仍未準備就緒。")
    return False

def delete_gcs_file(bucket_name, file_name):
    # 獲取指定的 Bucket
    bucket = storage_client.bucket(bucket_name)

    # 獲取要刪除的檔案物件
    blob = bucket.blob(file_name)

    # 刪除檔案
    blob.delete()
    return logging.info(f"檔案 {file_name} 已從 Bucket {bucket_name} 中刪除。")