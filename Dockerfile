# 使用官方 Python 基礎映像
FROM python:3.9-slim

# 設定工作目錄
WORKDIR /app

# 複製程式碼和依賴
COPY . /app

# 安裝依賴
RUN pip install --no-cache-dir -r requirements.txt

# 指定執行命令
CMD ["python", "main.py"]
