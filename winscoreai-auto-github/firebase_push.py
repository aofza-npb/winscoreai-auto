import os
import json
import firebase_admin
from firebase_admin import credentials, db

# ===== init Firebase Admin =====
# ถ้าใช้ GitHub Secret ที่เป็น JSON ดิบ (เดิมของคุณ)
firebase_key_json = os.environ["FIREBASE_ADMIN_KEY"]
firebase_key_dict = json.loads(firebase_key_json)

if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_key_dict)
    firebase_admin.initialize_app(cred, {
        "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
    })

# ------------------------------------------------------------
# A) ใช้กับ "UNDERSTAT SCRAPER" เดิม -> เขียนไป understat_agg/{team}/{YYYY}/{MM}/{DD}
#    (นี่คือการแทนที่ของเดิมที่เคยเขียนไป predictions/{match_id})
# ------------------------------------------------------------
def push_understat_agg(data: dict, team_slug: str, date_str: str):
    """
    team_slug: เช่น 'ac_milan'
    date_str : 'YYYY-MM-DD'
    """
    yyyy, mm, dd = date_str.split("-")
    path = f"understat_agg/{team_slug}/{yyyy}/{mm}/{dd}"
    ref = db.reference(path)
    ref.set(data)
    print(f"✅ understat_agg saved: {path}")

# (ทางลัดเพื่อไม่พังของเดิม ถ้าโค้ดที่อื่นยังเรียก push_prediction)
# ให้ชี้ไป understat_agg แทนชั่วคราว
def push_prediction(data: dict, match_id: str):
    """
    DEPRECATED: เดิมใช้เขียนไป predictions/{match_id}
    ตอนนี้เปลี่ยนให้ไป understat_agg โดย match_id ต้องเป็น 'teamSlug_YYYY-MM-DD'
    เช่น 'ac_milan_2014-10-26'
    """
    try:
        team_slug, date_str = match_id.rsplit("_", 1)
        push_understat_agg(data, team_slug, date_str)
    except Exception as e:
        print(f"❌ push_prediction (compat) failed for {match_id} | {e}")

# ------------------------------------------------------------
# B) ฟังก์ชันใหม่สำหรับ "ผล AI" -> predictions_ai/{date}/{fixture_id}
#    จะใช้ใน pipeline โมเดล AI เท่านั้น
# ------------------------------------------------------------
def push_ai_prediction(ai_data: dict, date_str: str, fixture_id: str):
    """
    ai_data ควรมี fields เช่น:
    {
      "home": "...", "away": "...",
      "lambda_home": 1.62, "lambda_away": 1.08,
      "p_over25": 0.64, "p_btts": 0.57, "p_home_hdp_-0.5": 0.61,
      "pick_main": "ต่อเจ้าบ้าน -0.5", "pick_ou": "Over 2.5",
      "confidence_pct": 86, "edge_pct": 7.5, "stars": 4,
      "reasons": ["...", "..."]
    }
    """
    path = f"predictions_ai/{date_str}/{fixture_id}"
    ref = db.reference(path)
    ref.set(ai_data)
    print(f"✅ predictions_ai saved: {path}")
