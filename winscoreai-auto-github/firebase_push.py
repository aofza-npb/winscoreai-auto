import os
import json
import firebase_admin
from firebase_admin import credentials, db

# ✅ โหลดค่า JSON จาก GitHub Secret ที่ชื่อ FIREBASE_ADMIN_KEY
firebase_key_json = os.environ["FIREBASE_ADMIN_KEY"]
firebase_key_dict = json.loads(firebase_key_json)

cred = credentials.Certificate(firebase_key_dict)
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
})

def push_prediction(data: dict, match_id: str):
    ref = db.reference(f"predictions/{match_id}")
    ref.set(data)
    print(f"✅ บันทึกสำเร็จ: {match_id}")
