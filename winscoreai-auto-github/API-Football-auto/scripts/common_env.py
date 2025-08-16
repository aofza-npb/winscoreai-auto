# common_env.py
# โหลด ENV/Secrets ให้ใช้ได้ทั้งโลคอลและ CI และรวม helper พื้นฐานที่ใช้ซ้ำ
import os, json, base64
from pathlib import Path
from datetime import datetime, timezone

try:
    # โหลด .env ถ้ามี (รันโลคอล)
    from dotenv import load_dotenv
    # .env อยู่ที่โฟลเดอร์ API-Football/
    load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass

# === API-Football ===
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY")
BASE_AF = "https://v3.football.api-sports.io"

def api_headers():
    if not API_FOOTBALL_KEY:
        raise RuntimeError("Missing API_FOOTBALL_KEY")
    return {"x-apisports-key": API_FOOTBALL_KEY}

# === Firebase ===
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL")  # ต้องตั้งใน Secrets/ENV
FIREBASE_CREDENTIALS  = os.getenv("FIREBASE_CREDENTIALS")   # base64 ของ service account JSON บน CI
FIREBASE_SA_PATH      = os.getenv("FIREBASE_SA_PATH", "firebase_service_account.json")  # สำหรับโลคอล

def get_firebase_cred_dict() -> dict:
    """
    - CI: ตั้ง FIREBASE_CREDENTIALS = base64(serviceAccount.json)
    - Local: วางไฟล์ service account แล้วตั้ง FIREBASE_SA_PATH ชี้ไฟล์
    """
    if FIREBASE_CREDENTIALS:
        try:
            return json.loads(base64.b64decode(FIREBASE_CREDENTIALS).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Invalid FIREBASE_CREDENTIALS (base64): {e}")
    p = Path(FIREBASE_SA_PATH)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    raise RuntimeError("No Firebase credentials: set FIREBASE_CREDENTIALS (base64) or FIREBASE_SA_PATH")

def get_firebase_db_url() -> str:
    if not FIREBASE_DATABASE_URL:
        # ใส่ fallback สำหรับรันโลคอล (ปรับเป็นของโปรเจกต์คุณ)
        return "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app"
    return FIREBASE_DATABASE_URL

# === Utilities ===
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def read_allowlist(path: str) -> list[int]:
    lids = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            p = s.split()
            if p[0].isdigit():
                lids.append(int(p[0]))
    return lids
