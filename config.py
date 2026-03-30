import os

# Power BI embed URL — this is a publicly accessible dashboard, not a secret.
POWERBI_EMBED_URL = os.getenv(
    "POWERBI_EMBED_URL",
    "https://app.powerbi.com/view?r="
    "eyJrIjoiOThhYTMyN2ItMGNjMS00NDIzLWFhM2QtMjkzNmZkNjFiM2E5IiwidCI6ImU1YzM3OTgx"
    "LTY2NjQtNDEzNC04YTBjLTY1NDNkMmFmODBiZSIsImMiOjh9",
)

MAX_ROWS_PER_REQUEST = 30000
DATA_VOLUME = 6
REQUEST_TIMEOUT = 60

# Firebase — no defaults; users must provide their own credentials.
FIREBASE_CRED_PATH = os.getenv("FIREBASE_CRED_PATH", "")
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL", "")
FIREBASE_COLLECTION = "powerbi_data"

SCHEDULE_INTERVAL_HOURS = int(os.getenv("SCHEDULE_INTERVAL_HOURS", "6"))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
