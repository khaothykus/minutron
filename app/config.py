import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR", "/app/data")
TEMPLATE_PATH = os.getenv("TEMPLATE_PATH", "/app/templates/minuta_template.xlsx")
RAT_URL = os.getenv("RAT_URL")
FIREFOX_BINARY = os.getenv("FIREFOX_BINARY")
GECKODRIVER_PATH = os.getenv("GECKODRIVER_PATH")

os.makedirs(BASE_DATA_DIR, exist_ok=True)
os.makedirs(f"{BASE_DATA_DIR}/users", exist_ok=True)
os.makedirs(f"{BASE_DATA_DIR}/logs", exist_ok=True)