import os
import logging
import time
import asyncio
from quart import Quart, request
from dotenv import load_dotenv
from services.syncData import sync_data_main
from services.updateGrp import update_grp_main
from services.generate import vertex_main
from util import check_gcs_file_ready
# Certs
# export GOOGLE_APPLICATION_CREDENTIALS="dla-dataform.json"
#storage_client = storage.Client.from_service_account_json("dla-dataform.json")
# 自動引用環境變數檔案
load_dotenv(verbose=True)

app = Quart(__name__)

@app.route("/syncData", methods=["POST"])
async def handle_request():
    try:
        event_data = request.get_json()
        event_id = event_data.get('id')
        # 檢查GCS批次檔案是否準備好
        logging.info("Received event:%s", event_data)
        logging.info("event id: %s", event_id)
        logging.info("object createtime: %s", event_data.get('timeCreated'))
        asyncio.create_task(process_sync_data())
        logging.info("Success start sync data cloudrun service.")
        return "Success start sync data cloudrun service.", 200
    except Exception as e:
        logging.error(f"syncdata request發生錯誤: {e}")
        raise
async def process_sync_data():
    try:
        gcs_status = await check_gcs_file_ready(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))
        if not gcs_status:
            logging.info("Returning: GCS 檔案尚未準備就緒")
            return logging.info(f"GCS 檔案 {os.getenv('DAILY_FILE')} 尚未準備就緒，請稍後再試。")
        sync_data_main()
        return logging.info("Success syncing batch data.")
    except Exception as e:
            logging.error(f"syncdata request發生錯誤: {e}")
            raise
@app.route("/updateGrp", methods=["POST"])
def handle_request_update_grp_meta():
    update_grp_main()
    return "Update Group Meta successfully", 200

@app.route("/generate", methods=["POST"])
def handle_request_generate():
    vertex_main()
    return "Success Gen Group Marketing Copywriting.", 200


async def main():
    # 使用 app.run_task() 啟動 Quart
    port = int(os.environ.get("PORT", 8080))
    await app.run_task(host="0.0.0.0", port=port, debug=True)

if __name__ == "__main__":
    asyncio.run(main())
# # 開出port號
# if __name__ == "__main__":
#     app.run_async(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
