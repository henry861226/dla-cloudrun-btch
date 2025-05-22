import os
import logging
import asyncio
import uvicorn
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from services.updateGrp import update_grp_main
from services.generate import vertex_main
from services.manualTrigger import manual_trigger_main
from util import check_gcs_file_ready, clean_gcs_file, call_llm_sp
from pydantic import BaseModel, Field, field_validator, ValidationInfo

from services import triggerllm, syncData
import json
# Certs
# export GOOGLE_APPLICATION_CREDENTIALS="dla-dataform.json"
#storage_client = storage.Client.from_service_account_json("dla-dataform.json")
# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

app = FastAPI()

@app.post('/clean-gcs')
def clean_gcs():
    try:
        clean_gcs_file(os.getenv('TEST_BUCKET'), 'upload')
        return "Success Clean up GCS bucket files", 200
    except Exception as e:
        logging.error(f"checking request發生錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/trigger_llm')
async def manual_trigger_llm():
    try:
        asyncio.create_task(manual_trigger_main())
        asyncio.create_task(call_llm_sp())
        return JSONResponse(status_code=200, content={"DESC": "SUCCESS GENERATE MARKETING-COPY."})
    except Exception as e:
        logging.error(f"manual trigger發生錯誤: {e}")
        return JSONResponse(status_code=200, content=str(e))

# # 後台觸發生成標籤群組文案
# class LlmRequestModel(BaseModel):
#     groupList: list[str] = Field(default = [])
# @app.post('/generate')
# def handle_request_generate(data: LlmRequestModel):
#     try:
#         call_llm_sp(data.groupList)
#         return JSONResponse(status_code=200, content={"desc": "SUCCESS GENERATE MARKETING-COPY."})
#     except Exception as e:
#         logging.error(f"發生錯誤: {e}")
#         return JSONResponse(status_code=500, content={"error": str(e)})
app.include_router(triggerllm.router, prefix="/test_route")
app.include_router(syncData.router, prefix="/test_sync")

# @app.post('/sync-data')
# async def handle_request():
#     try:
#         asyncio.create_task(process_sync_data())
#         logging.info("Success start sync data cloudrun service.")
#         return "Success start sync data cloudrun service.", 200
#     except Exception as e:
#         logging.error(f"syncdata request發生錯誤: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
# async def process_sync_data():
#     try:
#         gcs_status = await check_gcs_file_ready(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))
#         if not gcs_status:
#             logging.info("Returning: GCS 檔案尚未準備就緒")
#             return logging.info(f"GCS 檔案 {os.getenv('DAILY_FILE')} 尚未準備就緒，請稍後再試。")
#         await sync_data_main()
#         return logging.info("Success syncing batch data.")
#     except Exception as e:
#             logging.error(f"syncdata request發生錯誤: {e}")
#             raise

# FastAPI Start
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
