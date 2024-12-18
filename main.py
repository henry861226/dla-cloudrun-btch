import os
from flask import Flask, request
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

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handle_request():
    # 檢查GCS批次檔案是否準備好
    if not check_gcs_file_ready(os.getenv('GCS_BUCKET'), os.getenv('DAILY_FILE')):
        print("222")
        return f"GCS 檔案 {os.getenv('DAILY_FILE')} 尚未準備就緒，請稍後再試。", 400
    sync_data_main()
    return "Success syncing batch data.", 200

@app.route("/updateGrp", methods=["POST"])
def handle_request_update_grp_meta():
    update_grp_main()
    return "Update Group Meta successfully", 200

@app.route("/generate", methods=["POST"])
def handle_request_generate():
    vertex_main()
    return "Success Gen Group Marketing Copywriting.", 200

# 開出port號
app.run(port=int(os.environ.get("PORT", 8080)),host='0.0.0.0',debug=True)
