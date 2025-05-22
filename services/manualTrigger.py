import logging
import os
import asyncio
from dotenv import load_dotenv
from google.cloud import bigquery
from util import execute_bq_query, get_current_time

# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()
# 自動引用環境變數檔案
load_dotenv(verbose=True)

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def manual_trigger_main():
    # 取得最新客戶群組對應表日期
    exec_date = await check_sts_table()
    logging.info(f"Date: {exec_date}")

    # 比對客戶標籤，取得客戶的標籤群組
    match_tag_group(exec_date)

    # 寫入 Audit log
    add_audit_log(exec_date)

    return logging.info("Success trigger cust grp mapping.")

async def check_sts_table():
    query= f"""
        SELECT exec_date FROM `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.JOB_STS`
        WHERE status=true
        ORDER BY exec_date DESC
        LIMIT 1
    """
    result = next(bigquery_client.query(query).result(), None)
    exec_date = result.exec_date if result else None
    logging.info(f"Latest Enable Table: CUST_GRP_MAP_{exec_date}")
    return exec_date

def match_tag_group(exec_date):
    execute_bq_query(
        template_name='MATCH_TAG_GRP.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'date': exec_date
        }
    )
    return logging.info("Matching cust tags and tag groups.")

def add_audit_log(exec_date):
    execute_bq_query(
        template_name='ADD_AUDIT_LOG.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'datetime': get_current_time("%Y%m%d%H%M"),
            'date': exec_date
        }
    )
    return logging.info("Add audit log.")