# winscoreai-auto-github/tools/sync_team_mapping.py
import os, json, csv
from pathlib import Path

# 1) พยายาม reuse init จาก firebase_push ก่อน (ของเดิมที่เคยใช้ได้)
try:
    from firebase_push import db, firebase_admin  # ใช้ตัวเดียวกับโปรเจกต์
except Exception:
    db = None
    firebase_admin = None

# 2) ถ้ายังไม่มี app → init แบบ fallback ด้วย ENV
if not getattr(firebase_admin, "_apps", []):
    import firebase_admin as _fb
    from firebase_admin import credentials, db as _db

    key_json = os.environ.get("FIREBASE_ADMIN_KEY")
    if not key_json:
        raise RuntimeError("FIREBASE_ADMIN_KEY not found in env")

    cred = credentials.Certificate(json.loads(key_json))
    _fb.initialize_app(cred, {
        "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"  # ✅ ของโปรเจกต์คุณ
    })
    db = _db  # ใช้ db ที่ init ใหม่

CANDIDATES = [
    Path("winscoreai-auto-github/team_mapping/eng_to_th.csv"),
    Path("team_mapping/eng_to_th.csv"),
    Path("understat_scraper_auto/team_mapping/eng_to_th.csv"),
]

def load_mapping():
    path = next((p for p in CANDIDATES if p.exists()), None)
    if not path:
        print("❌ ไม่พบไฟล์ eng_to_th.csv"); return {}
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        cols = [c.strip() for c in (rdr.fieldnames or [])]
        mapping = {}
        for row in rdr:
            if {"eng","th"}.issubset(cols):
                k, v = row.get("eng"), row.get("th")
            elif {"eng_name","th_name"}.issubset(cols):
                k, v = row.get("eng_name"), row.get("th_name")
            else:
                keys = list(row.keys()); 
                k = row.get(keys[0]) if keys else None
                v = row.get(keys[1]) if len(keys) > 1 else None
            if isinstance(k, str) and isinstance(v, str) and k.strip():
                mapping[k.strip()] = v.strip()
    print(f"📦 loaded mapping: {len(mapping)} items from {path}")
    return mapping

def main():
    mapping = load_mapping()
    if not mapping: return
    db.reference("team_mapping/eng_to_th").set(mapping)
    print("✅ synced to Firebase")

if __name__ == "__main__":
    main()
