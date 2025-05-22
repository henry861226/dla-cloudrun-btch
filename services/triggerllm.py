import os
import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from google.cloud import bigquery

router = APIRouter()

# 初始化 BQ 客戶端
bigquery_client = bigquery.Client()

class LlmRequestModel(BaseModel):
    groupList: list[str] = Field(default = [])
@router.post("/llm")
def handle_request_generate(data: LlmRequestModel):
    try:
        logging.info(f"GROUP LIST: {data.groupList}")

        query = f"CALL `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET')}.gen_llm`({data.groupList})"
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info("完成文案生成！")
        return JSONResponse(status_code=200, content={"desc": "SUCCESS GENERATE MARKETING-COPY."})
    except Exception as e:
        logging.error(f"發生錯誤: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
