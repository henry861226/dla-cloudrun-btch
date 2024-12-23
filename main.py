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

@app.route("/test", methods=["POST"])
async def test():
    print("test start")
    #asyncio.create_task(try_async())
    
    # task.add_done_callback(on_task_done)
    # 在合適的地方呼叫
    #await cleanup_tasks()
    await check_all_tasks()
    print("trying...")
    
    logging.info("TEST async")
    # try_async()
    return "test sleep.", 200
def on_task_done(task):
    """當任務完成時執行的回調函數"""
    if task.exception():
        print(f"Task raised an exception: {task.exception()}")
    else:
        print(f"Task completed successfully, result: {task.result()}")
        # 在任務執行過程中，檢查當前的所有任務
async def try_async():
    try:
        print("資料檢查開始")
        #result = await test_check(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE'))
        # if result:
        #     print("資料檢查完成！")
        # else:
        #     print("檔案尚未準備就緒。")
    except Exception as e:
        logging.error(f"Error in try_async: {e}")
        raise  # 如果需要重新拋出錯誤
async def check_all_tasks():
    all_tasks = asyncio.all_tasks()
    for task in all_tasks:
        print(f"Task {task} is {task._state}")
# 用於清理任務，強制取消所有正在執行的任務
async def cleanup_tasks():
    # 獲取當前的所有任務
    all_tasks = asyncio.all_tasks()
    
    # 取消所有未完成的任務
    for task in all_tasks:
        if not task.done():
            print(f"Cancelling task {task}")
            task.cancel()
    # 等待所有任務完成或取消
    await asyncio.gather(*all_tasks, return_exceptions=True)
def handle_exit_signal(signum, frame):
    print("Caught exit signal, cleaning up tasks.")
    asyncio.create_task(cleanup_tasks())

@app.route("/syncData", methods=["POST"])
async def handle_request():
    try:
        asyncio.create_task(process_sync_data())
        logging.info("Success start sync data cloudrun service.")
        return "Success start sync data cloudrun service.", 200
    except Exception as e:
        logging.error(f"syncdata request發生錯誤: {e}")
        raise
async def process_sync_data():
    try:
        event_data = request.get_json()
        event_id = event_data.get('id')
        # 檢查GCS批次檔案是否準備好
        logging.info("Received event:%s", event_data)
        logging.info("event id: %s", event_id)
        logging.info("object createtime: %s", event_data.get('timeCreated'))
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

# 開出port號
if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
