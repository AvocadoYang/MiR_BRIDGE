from datetime import datetime

def format_date() -> str:
    now = datetime.now()
    return now.strftime('%Y-%m-%d-%H-%M-%S')

