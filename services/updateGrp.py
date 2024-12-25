import logging
import os
import asyncio
from google.cloud import bigquery
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from google.cloud import logging_v2
from util import get_current_time, check_table_exist, execute_bq_query

# 自動引用環境變數檔案
load_dotenv(verbose=True)
# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()
# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# 設置query模板文件目錄
env = Environment(loader=FileSystemLoader('query'))
# 執行當天日期 / 時間
current_date = get_current_time("%Y%m%d")
current_datetime = get_current_time("%Y%m%d%H%M")

async def update_grp_main():
    # 同步新的GROUP_META
    await sync_grp_meta_from_gcs()
    
    # 取得上傳人員資訊
    executor= await get_upload_user(os.getenv('GCS_GRP_BUCKET'), os.getenv('GROUP_FILE'))

    # 更新GROUP META，並新增此次異動紀錄至GRP_AUDIT_LOG
    await add_grp_meta_audit(executor)

    return logging.info("Update Group Meta successfully.")

async def sync_grp_meta_from_gcs():
    execute_bq_query(
        template_name='UPDATE_GRP_META.sql',
        render_params={
            'projectId': os.getenv('PROJECT_ID'),
            'dataset': os.getenv('DATASET'),
            'gcs_bucket': os.getenv('GCS_GRP_BUCKET'), 
            'group_file': os.getenv('GROUP_FILE')
        }
    )
    return logging.info("syncing gcs group meta csv data.")

async def get_upload_user(bucket_name, object_name):
    client = logging_v2.Client()

    # 定義日誌查詢
    query = f"""
        logName="projects/{os.getenv('PROJECT_ID')}/logs/cloudaudit.googleapis.com%2Fdata_access"
        resource.type="gcs_bucket"
        resource.labels.bucket_name="{bucket_name}"
        protoPayload.methodName="storage.objects.create"
        protoPayload.resourceName="projects/_/buckets/{bucket_name}/objects/{object_name}"
    """

    # 執行查詢
    for entry in client.list_entries(filter_=query):
        payload = entry.to_api_repr()
        user_email = payload.get("protoPayload", {}).get("authenticationInfo", {}).get("principalEmail")
        if user_email:
            logging.info(user_email)
            return user_email

    return "Unknown user"

async def add_grp_meta_audit(executor):

    table_ref = bq_client.dataset(os.getenv('DATASET')).table('GROUP_META')
    table = bq_client.get_table(table_ref)
    columns = [schema_field.name for schema_field in table.schema]
    
    # 構建動態的 MERGE 插入語句
    merge_query = f"MERGE `{os.getenv('DATASET')}.GROUP_META` T USING `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_NEW` S ON T.group_uuid = S.group_uuid\n"

    # 用於更新表的語句
    merge_actions = []

    # 用於插入變更記錄的查詢
    audit_log_inserts = []

    for column in columns:
        if column == "tags":  # 假设 ARRAY<STRING> 的列名为 array_column_name
        # 更新 GROUP_META 的 MERGE 子句
            merge_actions.append(f"""
            WHEN MATCHED AND ARRAY_TO_STRING(T.{column}, ',') != ARRAY_TO_STRING(S.{column}, ',') THEN
            UPDATE SET T.{column} = S.{column}
            """)
            # 插入 GRP_AUDIT_LOG 的獨立查詢
            audit_log_inserts.append(f"""
            SELECT
                '{current_datetime}' AS create_datetime,
                '{executor}' AS executor,
                T.group_uuid AS group_uuid,
                "{column}" AS column_name,
                ARRAY_TO_STRING(T.{column}, ',') AS previous_value,
                ARRAY_TO_STRING(S.{column}, ',') AS new_value
            FROM `{os.getenv('DATASET')}.GROUP_META` T
            JOIN `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_NEW` S
            ON T.group_uuid = S.group_uuid
            WHERE ARRAY_TO_STRING(T.{column}, ',') != ARRAY_TO_STRING(S.{column}, ',')
            """)
        else:
            # 更新 GROUP_META 的 MERGE 子句
            merge_actions.append(f"""
            WHEN MATCHED AND T.{column} != S.{column} THEN
            UPDATE SET T.{column} = S.{column}
            """)
            # 插入 GRP_AUDIT_LOG 的獨立查詢
            audit_log_inserts.append(f"""
            SELECT
                '{current_datetime}' AS create_datetime,
                '{executor}' AS executor,
                T.group_uuid AS group_uuid,
                "{column}" AS column_name,
                CAST(T.{column} AS STRING) AS previous_value,
                CAST(S.{column} AS STRING) AS new_value
            FROM `{os.getenv('DATASET')}.GROUP_META` T
            JOIN `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_NEW` S
            ON T.group_uuid = S.group_uuid
            WHERE T.{column} != S.{column}
            """)

    merge_query += "\n".join(merge_actions)

    # 拼接 INSERT 查詢
    audit_log_query = f"""
    INSERT INTO `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GRP_AUDIT_LOG`
    { " UNION ALL ".join(audit_log_inserts) }
    """

    # 刪除 source_table 的查詢
    drop_source_table_query = f"DROP TABLE `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_NEW`"

    check_table_exist('GROUP_NEW')
    # Grp auditlog
    result = bq_client.query(audit_log_query).result()
    affected_rows = result.num_dml_affected_rows
    logging.info(f"欄位更新總數: {affected_rows}")
    # Merging grp meta
    merge_result = bq_client.query(merge_query).result()
    merge_affected_rows = merge_result.num_dml_affected_rows
    logging.info(f"標籤群組更新總數: {merge_affected_rows}")
    # Drop temp table
    bq_client.query(drop_source_table_query).result()
    return logging.info("Adding grp meta audit log.")