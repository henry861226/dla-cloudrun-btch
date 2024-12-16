import logging
import os
import time
import vertexai
import json
from vertexai.generative_models import GenerativeModel
from vertexai.batch_prediction import BatchPredictionJob
from google.cloud import bigquery
from dotenv import load_dotenv

# 自動引用環境變數檔案
load_dotenv(verbose=True)
# 初始化 BigQuery 客戶端
bq_client = bigquery.Client()

# 設置日誌格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 初始化 Vertex AI
vertexai.init(project=os.getenv("PROJECT_ID"), location="us-central1")

def vertex_main():
    # 1. 從 BigQuery 獲取數據
    bq_data = fetch_bq_data()
    # 2. 使用 LLM 生成數據
    for index, record in enumerate(bq_data):
        prompt = f"""
            使用以下資訊生成文本：
            標籤: {record['tags']}
            合規要求: {record['compliance']}
            題目: {record['prompt']}
        """
        generated_text = generate_llm_output(prompt).strip()
        print(f"===== generated {index} text =====")
        print(generated_text)
        print("================")
        update_llm_text(record['group_uuid'], generated_text)

    # Generative AI on Vertex AI batch prediction
    #test_insert()
    #vertex_batch_predic()
    
    return logging.info("Completing GROUP_META LLM Generating.")

def fetch_bq_data():
    """從 BigQuery 資料表中獲取數據"""
    query = f"""
    SELECT 
        group_uuid, 
        ARRAY_TO_STRING(ARRAY(SELECT tags FROM UNNEST(tags) AS tags LIMIT 10), ',') AS tags, 
        compliance, 
        prompt 
    FROM `{os.getenv("PROJECT_ID")}.{os.getenv("DATASET")}.GROUP_META`
    """
    query_job = bq_client.query(query)
    rows = query_job.result()
    return [{"group_uuid": row.group_uuid, "tags": row.tags, "compliance": row.compliance, "prompt": row.prompt,} for row in rows]

def generate_llm_output(prompt):
    """使用 GenerativeModel 生成數據"""
    gen_config= {
        "temperature": 0.5,          # 控制生成的隨機性
        "max_output_tokens": 1024,   # 限制生成文字的最大字數
        "top_k": 40,                 # 用於限制候選 token 數
        "top_p": 0.5                 # 依據累積概率選取候選 token
    }
    model = GenerativeModel(
        "gemini-1.5-flash-002",
        generation_config=gen_config
    )
    response = model.generate_content(prompt)
    return response.text

def update_llm_text(group_uuid, llm_text):
    """將 LLM 生成的數據回寫到 GROUP_META 表中"""
    query = f"""
    UPDATE `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.GROUP_META`
    SET marketing_copy = "{llm_text}"
    WHERE group_uuid = "{group_uuid}"
    """
    print(group_uuid)
    bq_client.query(query).result()

# Generative AI on Vertex AI (批次預測)
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
# Create testing data for Vertex AI Batch Prediction Format
def test_insert():
    # 批量生成 JSON 数据
    rows_to_insert = []
    for i in range(0,1000):
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
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if not errors:
        print("9996 records inserted successfully.")
    else:
        print(f"Errors occurred: {errors}")
