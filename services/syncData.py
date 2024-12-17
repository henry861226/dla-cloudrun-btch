import logging
import os
from dotenv import load_dotenv
from util import get_current_time, check_table_exist, check_gcs_file_ready, delete_gcs_file, execute_bq_query

# 自動引用環境變數檔案
load_dotenv(verbose=True)

# 執行當天日期 / 時間
current_date = get_current_time("%Y%m%d")
current_datetime = get_current_time("%Y%m%d%H%M")

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def sync_data_main():
    """處理 HTTP 請求，觸發工作流"""

    # 檢查GCS批次檔案是否準備好
    if not check_gcs_file_ready(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE')):
        return f"GCS 檔案 {os.getenv('DAILY_FILE')} 尚未準備就緒，請稍後再試。", 400

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
    execute_bq_query(
        template_name='SYNC_CSV.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'gcs_bucket': os.getenv('GCS_BUCKET'),
            'daily_file': os.getenv('DAILY_FILE')
        }
    )
    return logging.info("syncing gcs daily csv data.")

def tag_compare():
    execute_bq_query(
        template_name='TAG_COMPARE.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'date': current_date
        }
    )
    return logging.info("Comparing cust tags threadholds.")

def match_tag_group():
    check_table_exist(f"CUST_TAGS_{current_date}")
    execute_bq_query(
        template_name='MATCH_TAG_GRP.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'date': current_date
        }
    )
    return logging.info("Matching cust tags and tag groups.")

def add_audit_log():
    execute_bq_query(
        template_name='ADD_AUDIT_LOG.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'datetime': current_datetime,
            'date': current_date
        }
    )
    return logging.info("Add audit log.")
