import logging
import os
import asyncio
from dotenv import load_dotenv
from util import get_current_time, check_table_exist, delete_gcs_file, execute_bq_query

# 自動引用環境變數檔案
load_dotenv(verbose=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def sync_data_main():
    try:
        await asyncio.sleep(1)
        """處理 HTTP 請求，觸發工作流"""

        # 1. 確認狀態表當日狀態，並新增/更新狀態
        job_sts = merge_job_sts()
        if job_sts is False:
            return logging.info("Previous Batch still running. Please wait!")
        
        # 2. 從 GCS 同步CSV檔案
        sync_data_from_gcs()

        # 3. 轉換資料格式
        trans_external_data()

        # 4. 進行標籤比對，比對客戶個人標籤
        tag_compare()

        # 5. 比對客戶標籤，取得客戶的標籤群組
        match_tag_group()

        # 6. 寫入 Audit log
        add_audit_log()

        # 7. 執行完成，更新狀態表當日狀態
        complete_job_sts()

        # 8. 刪除 GCS 檔案
        delete_gcs_file(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))

        return logging.info("Success sync batch data step.")
    except Exception as e:
            logging.error(f"sync data發生錯誤: {e}")
            raise

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

def trans_external_data():
    execute_bq_query(
        template_name='TRANS_CSV.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET')
        }
    )
    return logging.info("transform original csv data.")

def merge_job_sts():
    job_sts = execute_bq_query(
        template_name='CHECK_STS.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'current_date': get_current_time("%Y%m%d")
         }
    )
    rows = list(job_sts)
    if rows:
        if rows[0]["status"]:
            logging.info(f"Status {rows[0]['status']}, rerun batch can run.")
            execute_bq_query(
                template_name='MERGE_STS.sql',
                render_params={
                'projectId': os.getenv('PROJECT_ID'),
                'dataset': os.getenv('DATASET'),
                'current_date': get_current_time("%Y%m%d"),
                'status': False
                }
            )
        else:
            return False
    else:
        logging.info(f"Today first batch run. ")
        execute_bq_query(
            template_name='MERGE_STS.sql',
            render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'current_date': get_current_time("%Y%m%d"),
            'status': False
            }
        )
    return logging.info("update/insert job daily sts.")

def tag_compare():
    execute_bq_query(
        template_name='TAG_COMPARE.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'date': get_current_time("%Y%m%d")
        }
    )
    return logging.info("Comparing cust tags threadholds.")

def match_tag_group():
    check_table_exist(f"CUST_TAGS_{get_current_time('%Y%m%d')}")
    execute_bq_query(
        template_name='MATCH_TAG_GRP.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'date': get_current_time("%Y%m%d")
        }
    )
    return logging.info("Matching cust tags and tag groups.")

def complete_job_sts():
    execute_bq_query(
        template_name='MERGE_STS.sql',
        render_params={
        'projectId': os.getenv('PROJECT_ID'),
        'dataset': os.getenv('DATASET'),
        'current_date': get_current_time("%Y%m%d"),
        'status': True
        }
    )
    return logging.info("Update today job status.")

def add_audit_log():
    execute_bq_query(
        template_name='ADD_AUDIT_LOG.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'datetime': get_current_time("%Y%m%d%H%M"),
            'date': get_current_time("%Y%m%d")
        }
    )
    return logging.info("Add audit log.")
