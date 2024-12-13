from datetime import datetime

def get_current_time(format_str: str) -> str:
    """取得格式化時間"""
    return datetime.now().strftime(format_str)
