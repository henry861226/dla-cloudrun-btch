import logging
import os
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from util import get_current_time, check_table_exist, delete_gcs_file, execute_bq_query, call_llm_sp, check_gcs_file_ready

router = APIRouter()
# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# GCP Env
PROJECT_ID= os.getenv('PROJECT_ID')
BQ_DATASET= os.getenv('DATASET')
GCS_BUCKET= os.getenv('GCS_BUCKET')
BTH_FILE= f"upload/{os.getenv('BTCH_FILE')}"

@router.post('/sync-data')
def handle_request():
    try:
        # 0. Get Current Datetime
        CURRENT_DATE= get_current_time('%Y%m%d')
        CURRENT_DATETIME= get_current_time('%Y%m%d%H%M')

        # 1. Check GCS file.
        gcs_status = check_gcs_file_ready(GCS_BUCKET, BTH_FILE)
        if not gcs_status:
            logging.info("Returning: GCS 檔案尚未準備就緒")
            return JSONResponse(status_code=200, content={"desc": "GCS file not Ready."})
        # 2. 確認狀態表當日狀態，並新增/更新狀態
        job_sts = merge_job_sts(CURRENT_DATE)
        if job_sts is False:
            return JSONResponse(status_code=200, content={"desc": "Previous Batch still running. Please wait!."})
        
        # 3. 從 GCS 同步CSV檔案
        sync_data_from_gcs()

        # 4. 轉換資料格式
        trans_external_data()

        # 5. 進行標籤比對，比對客戶個人標籤
        tag_compare(CURRENT_DATE)

        # 6. 比對客戶標籤，取得客戶的標籤群組
        match_tag_group(CURRENT_DATE)

        # 7. 寫入 Audit log
        add_audit_log(CURRENT_DATE, CURRENT_DATETIME)

        # 8. 執行完成，更新狀態表當日狀態
        complete_job_sts(CURRENT_DATE)

        # 9. 刪除 GCS 檔案
        delete_gcs_file(GCS_BUCKET, BTH_FILE)

        return JSONResponse(status_code=200, content={"desc": "Success Mapping Cust Tag Group Batch Job."})
    except Exception as e:
        logging.error(f"syncdata request發生錯誤: {e}")
        raise JSONResponse(status_code=500, content={"error": str(e)})

def sync_data_from_gcs():
    execute_bq_query(
        template_name='SYNC_CSV.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'gcs_bucket': GCS_BUCKET,
            'daily_file': BTH_FILE
        }
    )
    return logging.info("syncing gcs daily csv data.")

def trans_external_data():
    execute_bq_query(
        template_name='TRANS_CSV.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET
        }
    )
    return logging.info("transform original csv data.")

def merge_job_sts(date):
    job_sts = execute_bq_query(
        template_name='CHECK_STS.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'current_date': date
         }
    )
    rows = list(job_sts)
    if rows:
        if rows[0]["status"]:
            logging.info(f"Status {rows[0]['status']}, rerun batch can run.")
            execute_bq_query(
                template_name='MERGE_STS.sql',
                render_params={
                'projectId': PROJECT_ID,
                'dataset': BQ_DATASET,
                'current_date': date,
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
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'current_date': date,
            'status': False
            }
        )
    return logging.info("update/insert job daily sts.")

def tag_compare(date):
    execute_bq_query(
        template_name='TAG_COMPARE.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'date': date
        }
    )
    return logging.info("Comparing cust tags threadholds.")

def match_tag_group(date):
    check_table_exist(f"CUST_TAGS_{date}")
    execute_bq_query(
        template_name='MATCH_TAG_GRP.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'date': date
        }
    )
    return logging.info("Matching cust tags and tag groups.")

def complete_job_sts(date):
    execute_bq_query(
        template_name='MERGE_STS.sql',
        render_params={
        'projectId': PROJECT_ID,
        'dataset': BQ_DATASET,
        'current_date': date,
        'status': True
        }
    )
    return logging.info("Update today job status.")

def add_audit_log(date, datetime):
    execute_bq_query(
        template_name='ADD_AUDIT_LOG.sql',
        render_params={
            'projectId': PROJECT_ID,
            'dataset': BQ_DATASET,
            'datetime': datetime,
            'date': date
        }
    )
    return logging.info("Add audit log.")