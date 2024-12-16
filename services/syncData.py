import logging
import os
from google.cloud import bigquery
from google.cloud import storage
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from util import get_current_time, check_table_exist

# 自動引用環境變數檔案
load_dotenv(verbose=True)
# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()
# 初始化 GCS 客戶端
storage_client = storage.Client()
# 設置query模板文件目錄
env = Environment(loader=FileSystemLoader('query'))

# 執行當天日期 / 時間
current_date = get_current_time("%Y%m%d")
current_datetime = get_current_time("%Y%m%d%H%M")

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def sync_data_main():
    """處理 HTTP 請求，觸發工作流"""
    # 1. 從 GCS 同步CSV檔案
    sync_data_from_gcs()

    # 2. 進行標籤比對，比對客戶個人標籤
    tag_compare()

    # 3. 比對客戶標籤，取得客戶的標籤群組
    match_tag_group()

    # 4. 寫入 Audit log
    add_audit_log()

    # 5. 刪除 GCS 檔案
    delete_gcs_file(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))

    return "Processed successfully", 200

def sync_data_from_gcs():
    template = env.get_template('SYNC_CSV.sql')
    # 渲染模板，并动态替换参数
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), gcs_bucket=os.getenv('GCS_BUCKET'), daily_file=os.getenv('DAILY_FILE'))
    bigquery_client.query(query).result()
    return logging.info("syncing gcs daily csv data.")

def tag_compare():
    template = env.get_template('TAG_COMPARE.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), date=current_date)
    bigquery_client.query(query).result()
    return logging.info("Comparing cust tags threadholds.")

def match_tag_group():
    template = env.get_template('MATCH_TAG_GRP.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), date=current_date)
    check_table_exist(f"CUST_TAGS_{current_date}")
    bigquery_client.query(query).result()
    return logging.info("Matching cust tags and tag groups.")

def add_audit_log():
    template = env.get_template('ADD_AUDIT_LOG.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), datetime=current_datetime,  date=current_date)
    bigquery_client.query(query).result()
    return logging.info("Add audit log.")

def delete_gcs_file(bucket_name, file_name):
    # 獲取指定的 Bucket
    bucket = storage_client.bucket(bucket_name)

    # 獲取要刪除的檔案物件
    blob = bucket.blob(file_name)

    # 刪除檔案
    blob.delete()
    return logging.info(f"檔案 {file_name} 已從 Bucket {bucket_name} 中刪除。")
