import logging
import os
import time
import vertexai
import json
from vertexai.generative_models import GenerativeModel
from vertexai.batch_prediction import BatchPredictionJob
from flask import Flask, request
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud import logging_v2
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from util import get_current_time

# Certs
# export GOOGLE_APPLICATION_CREDENTIALS="dla-dataform.json"
#storage_client = storage.Client.from_service_account_json("dla-dataform.json")
# 自動引用環境變數檔案
load_dotenv(verbose=True)

app = Flask(__name__)

# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()

# 設置query模板文件目錄
env = Environment(loader=FileSystemLoader('query'))

# 執行當天日期 / 時間
current_date = get_current_time("%Y%m%d")
current_datetime = get_current_time("%Y%m%d%H%M")

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@app.route("/", methods=["POST"])
def handle_request():
    """處理 HTTP 請求，觸發工作流"""
    # 1. 從 GCS 同步CSV檔案
    sync_data_from_gcs()

    # 2. 進行標籤比對，比對客戶個人標籤
    tag_compare()

    # 3. 比對客戶標籤，取得客戶的標籤群組
    match_tag_group()

    # 4. 寫入Audit log
    add_audit_log()
    return "Processed successfully", 200

@app.route("/updateGroupMeta", methods=["POST"])
def handle_request_update_grp_meta():
    # 同步新的GROUP_META
    sync_grp_meta_from_gcs()
    # 取得上傳人員資訊
    executor=get_upload_user(os.getenv('GCS_BUCKET'), os.getenv('GROUP_FILE'))
    # 更新GROUP META，並新增此次異動紀錄至GRP_AUDIT_LOG
    add_grp_meta_audit(executor)

    return "Update Group Meta successfully", 200

@app.route("/vertexPredic", methods=["POST"])
def handle_request_vertex_predic():
    #test_insert()
    vertex_batch_predic()
    return "Success Batch Predic", 200

def sync_data_from_gcs():
    template = env.get_template('SYNC_CSV.sql')
    # 渲染模板，并动态替换参数
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), gcs_bucket=os.getenv('GCS_BUCKET'), daily_file=os.getenv('DAILY_FILE'))
    bigquery_client.query(query)

    return logging.info("syncing gcs daily csv data.")
def sync_grp_meta_from_gcs():
    template = env.get_template('UPDATE_GRP_META.sql')
    # 渲染模板，并动态替换参数
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), gcs_bucket=os.getenv('GCS_BUCKET'), group_file=os.getenv('GROUP_FILE'))
    bigquery_client.query(query)

    return logging.info("syncing gcs group meta csv data.")

def tag_compare():
    template = env.get_template('TAG_COMPARE.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), date=current_date)
    bigquery_client.query(query)
    return logging.info("Comparing cust tags threadholds.")

def match_tag_group():
    template = env.get_template('MATCH_TAG_GRP.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), date=current_date)
    check_table_exist(f"CUST_TAGS_{current_date}")
    bigquery_client.query(query)
    return logging.info("Matching cust tags and tag groups.")

def add_audit_log():
    template = env.get_template('ADD_AUDIT_LOG.sql')
    query = template.render(projectId=os.getenv('PROJECT_ID'), dataset=os.getenv('DATASET'), datetime=current_datetime,  date=current_date)
    bigquery_client.query(query)
    return logging.info("Add audit log.")

def get_upload_user(bucket_name, object_name):
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

def add_grp_meta_audit(executor):

    table_ref = bigquery_client.dataset(os.getenv('DATASET')).table('GROUP_META')
    table = bigquery_client.get_table(table_ref)

    columns = [schema_field.name for schema_field in table.schema]
    print("test table schema: ", columns)
    
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
                '{current_date}' AS create_datetime,
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
                '{current_date}' AS create_datetime,
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

    # 打印生成的查詢
    print("Generated Merge Query:")
    print(merge_query)
    print("Generated Audit Log Query:")
    print(audit_log_query)
    check_table_exist('GROUP_NEW')
    bigquery_client.query(audit_log_query).result()
    bigquery_client.query(merge_query).result()
    bigquery_client.query(drop_source_table_query).result()
    return logging.info("Adding grp meta audit log.")

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

def vertex_batch_predic():
    # TODO(developer): Update and un-comment below line
    # PROJECT_ID = "your-project-id"

    # Initialize vertexai
    vertexai.init(project=os.getenv('PROJECT_ID'), location="asia-east1")

    input_uri = f"bq://{os.getenv('PROJECT_ID')}.vertex.test_predic"
    output_uri = f"bq://{os.getenv('PROJECT_ID')}.vertex.BTCH_PREDIC_100w"
    print("input_uri: ", input_uri)
    # Submit a batch prediction job with Gemini model
    batch_prediction_job = BatchPredictionJob.submit(
        source_model="gemini-1.5-pro-001",
        input_dataset=input_uri,
        output_uri_prefix=output_uri,
    )

    # Check job status
    print(f"Job resource name: {batch_prediction_job.resource_name}")
    print(f"Model resource name with the job: {batch_prediction_job.model_name}")
    print(f"Job state: {batch_prediction_job.state.name}")

    # Refresh the job until complete
    while not batch_prediction_job.has_ended:
        time.sleep(5)
        batch_prediction_job.refresh()

    # Check if the job succeeds
    if batch_prediction_job.has_succeeded:
        print("Job succeeded!")
    else:
        print(f"Job failed: {batch_prediction_job.error}")

    # Check the location of the output
    print(f"Job output location: {batch_prediction_job.output_location}")

    # Example response:
    #  Job output location: bq://Project-ID/gen-ai-batch-prediction/predictions-model-year-month-day-hour:minute:second.12345
def test_insert():
    # 批量生成 JSON 数据
    rows_to_insert = []
    for i in range(100004,100006):
        request_data = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": f"使用以下標籤:tag{i+1}，提供16字網路銀行行銷文案。"
                        }
                    ],
                    "role": "user"
                }
            ],
            "system_instruction": {
                "parts": [
                    {
                        "text": "你是個銀行市場行銷人員"
                    }
                ]
            }
        }
        rows_to_insert.append({"request": json.dumps(request_data)})

    # 构造表引用
    table_ref = f"{os.getenv('PROJECT_ID')}.vertex.test_predic"

    # 批量插入数据
    errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
    if not errors:
        print("9996 records inserted successfully.")
    else:
        print(f"Errors occurred: {errors}")
# 開出port號
app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
