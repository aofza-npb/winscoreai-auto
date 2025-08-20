# firebase_push.py
import os
import re
import json
import firebase_admin
from firebase_admin import credentials, db

# ---------- Firebase Admin init ----------
# ใช้ GitHub Secret ที่เป็น JSON ดิบ (เหมือนของคุณตอนนี้)
FIREBASE_KEY_JSON = os.environ["FIREBASE_ADMIN_KEY"]
FIREBASE_KEY_DICT = json.loads(FIREBASE_KEY_JSON)

if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY_DICT)
    firebase_admin.initialize_app(cred, {
        "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
    })

# ---------- A) Understat writer ----------
def push_understat_agg(data: dict, team_slug: str, date_str: str):
    """
    เขียนค่าเฉลี่ย/ตัวชี้วัดจาก Understat ไปไว้:
    understat_agg/{team_slug}/{YYYY}/{MM}/{DD}
    """
    yyyy, mm, dd = date_str.split("-")
    path = f"understat_agg/{team_slug}/{yyyy}/{mm}/{dd}"
    ref = db.reference(path)
    ref.set(data)
    print(f"✅ understat_agg saved: {path}")

# ---------- (Compat) เดิมเคยเรียก push_prediction ----------
def push_prediction(data: dict, match_id: str):
    """
    DEPRECATED: เดิมเขียน predictions/{match_id}
    ตอนนี้จะ forward ไป understat_agg โดยคาดว่า match_id รูปแบบ:
      team_slug_YYYY-MM-DD
    """
    try:
        if "_" not in match_id:
            raise ValueError("match_id must be 'team_slug_YYYY-MM-DD'")
        team_slug, date_str = match_id.rsplit("_", 1)
        push_understat_agg(data, team_slug, date_str)
    except Exception as e:
        print(f"❌ push_prediction (compat) failed: {match_id} | {e}")
def safe_key(key: str) -> str:
    """ทำให้ key ใช้ได้กับ Firebase"""
    if not key or not isinstance(key, str):
        return "unknown"
    # แทนที่ตัวอักษรต้องห้าม . $ # [ ] /
    key = re.sub(r'[.$#[\]/]', "_", key)
    return key.strip() or "unknown"

# ---------- B) AI predictions writer ----------
def push_ai_prediction(ai_data, date_str, fixture_id):
    from firebase_admin import db
    safe_fixture_id = safe_key(str(fixture_id))

    ref = db.reference(f"predictions/{safe_fixture_id}/{date_str}")
    ref.set(ai_data)
    print(f"✅ pushed prediction to predictions/{safe_fixture_id}/{date_str}")
    
def push_team_mapping_to_firebase(map_dict: dict, path: str = "team_mapping/eng_to_th"):
    ref = db.reference(path)
    ref.set(map_dict)
    print(f"✅ team_mapping saved: {path} ({len(map_dict)} items)")
