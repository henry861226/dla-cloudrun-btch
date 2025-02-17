import os
import logging
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from services.syncData import sync_data_main
from services.updateGrp import update_grp_main
from services.generate import vertex_main
from util import check_gcs_file_ready
# Certs
# export GOOGLE_APPLICATION_CREDENTIALS="dla-dataform.json"
#storage_client = storage.Client.from_service_account_json("dla-dataform.json")
# 自動引用環境變數檔案
load_dotenv(verbose=True, override=True)

app = FastAPI()
@app.post('/test-check')
async def check_test():
    try:
        await check_gcs_file_ready('testing_pcg_bucket', 'upload/input_data.csv')
        return "Success checking.", 200
    except Exception as e:
        logging.error(f"checking request發生錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/sync-data')
async def handle_request():
    try:
        asyncio.create_task(process_sync_data())
        logging.info("Success start sync data cloudrun service.")
        return "Success start sync data cloudrun service.", 200
    except Exception as e:
        logging.error(f"syncdata request發生錯誤: {e}")
        raise HTTPException(status_code=500, detail=str(e))
async def process_sync_data():
    try:
        gcs_status = await check_gcs_file_ready(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))
        if not gcs_status:
            logging.info("Returning: GCS 檔案尚未準備就緒")
            return logging.info(f"GCS 檔案 {os.getenv('DAILY_FILE')} 尚未準備就緒，請稍後再試。")
        await sync_data_main()
        return logging.info("Success syncing batch data.")
    except Exception as e:
            logging.error(f"syncdata request發生錯誤: {e}")
            raise

@app.post('/updateGrp')
async def handle_request_update_grp_meta():
    try:
        asyncio.create_task(process_update_grp())
        return "Update Group Meta successfully", 200
    except Exception as e:
        logging.error(f"updateData request發生錯誤: {e}")
        raise
async def process_update_grp():
    await update_grp_main()
    return logging.info("test async update grp process. ")

@app.post('/generate')
def handle_request_generate():
    vertex_main()
    return "Success Gen Group Marketing Copywriting.", 200

# async def main():
#     # 使用 app.run_task() 啟動 Quart
#     port = int(os.environ.get("PORT", 8080))
#     await app.run_task(host="0.0.0.0", port=port, debug=True)

# if __name__ == "__main__":
#     asyncio.run(main())
# FastAPI Start
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
