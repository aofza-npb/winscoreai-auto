# winscoreai-auto-github/tools/sync_team_mapping.py
# Sync team_mapping/eng_to_th.csv ขึ้น Firebase (รองรับหลาย header + กัน key พัง)

import os
import re
import csv
import json
from pathlib import Path

# ---------------------------
# Firebase init (reuse > fallback)
# ---------------------------
db = None
firebase_admin = None

try:
    # พยายามใช้ init เดิมจากโปรเจกต์ (จะมี databaseURL ครบ)
    from firebase_push import db as _db, firebase_admin as _fb  # type: ignore
    db = _db
    firebase_admin = _fb
except Exception:
    pass

if not getattr(firebase_admin, "_apps", []):
    # Fallback: init เองด้วย ENV
    import firebase_admin as _fb
    from firebase_admin import credentials, db as _db

    key_json = os.environ.get("FIREBASE_ADMIN_KEY")
    if not key_json:
        raise RuntimeError(
            "FIREBASE_ADMIN_KEY not found in env. "
            "Add it in GitHub Actions secrets or reuse firebase_push init."
        )

    cred = credentials.Certificate(json.loads(key_json))
    _fb.initialize_app(
        cred,
        {
            # ✅ เปลี่ยนให้ตรงกับโปรเจกต์ของคุณถ้า URL ต่างกัน
            "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
        },
    )
    db = _db
    firebase_admin = _fb


# ---------------------------
# Utils
# ---------------------------
def safe_key(key: str) -> str:
    """ทำให้ key ใช้ได้กับ Firebase (ห้าม . $ # [ ] / และห้ามว่าง)"""
    if not isinstance(key, str):
        return "unknown"
    key = key.strip()
    if not key:
        return "unknown"
    key = re.sub(r"[.$#[\]/]", "_", key)
    key = key.strip()
    return key or "unknown"


def detect_csv_path() -> Path | None:
    """หาไฟล์ mapping จากหลายตำแหน่งที่เป็นไปได้"""
    candidates = [
        Path("winscoreai-auto-github/team_mapping/eng_to_th.csv"),
        Path("understat_scraper_auto/team_mapping/eng_to_th.csv"),
        Path("team_mapping/eng_to_th.csv"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def load_mapping_from_csv(path: Path) -> dict[str, str]:
    """
    โหลด CSV รองรับ header:
      - eng, th
      - eng_name, th_name
    หรือถ้าไม่พบ headerที่รู้จัก -> ใช้ 2 คอลัมน์แรก
    """
    mapping: dict[str, str] = {}

    # เปิดแบบ universal newline + utf-8-sig กัน BOM
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return mapping

    # ดึง header (ถ้ามี)
    header = [c.strip() for c in rows[0]] if rows else []
    body = rows[1:] if rows else []

    # สร้าง DictReader ถ้ามี header
    if header and all(h != "" for h in header):
        dict_reader = csv.DictReader(
            ["\t".join(header)] + ["\t".join(r) for r in body],
            delimiter="\t",
        )
        cols = [c.strip() for c in dict_reader.fieldnames or []]

        def add_row(row: dict):
            if {"eng", "th"}.issubset(cols):
                k = row.get("eng", "")
                v = row.get("th", "")
            elif {"eng_name", "th_name"}.issubset(cols):
                k = row.get("eng_name", "")
                v = row.get("th_name", "")
            else:
                # ใช้สองคอลัมน์แรกที่เจอ
                keys = list(row.keys())
                k = row.get(keys[0], "") if keys else ""
                v = row.get(keys[1], "") if len(keys) > 1 else ""
            k = safe_key(str(k))
            v = str(v).strip()
            if k and v:
                mapping[k] = v

        for row in dict_reader:
            add_row(row)

    else:
        # ไม่มี header -> ใช้ 2 คอลัมน์แรก
        for r in body:
            if not r:
                continue
            k = safe_key(str(r[0]) if len(r) > 0 else "")
            v = str(r[1]).strip() if len(r) > 1 else ""
            if k and v:
                mapping[k] = v

    return mapping


# ---------------------------
# Main
# ---------------------------
def main():
    csv_path = detect_csv_path()
    if not csv_path:
        print("❌ ไม่พบไฟล์ eng_to_th.csv ในตำแหน่งที่คาดไว้")
        return

    mapping = load_mapping_from_csv(csv_path)

    # กันกรณีว่าง/ไม่มีข้อมูล
    if not mapping:
        print(f"⚠️  โหลดข้อมูลจาก {csv_path} ได้ 0 แถว — ยกเลิกการ sync")
        return

    # push เข้า Firebase
    ref = db.reference("team_mapping/eng_to_th")
    ref.set(mapping)
    print(f"✅ synced {len(mapping)} records from {csv_path} → team_mapping/eng_to_th")


if __name__ == "__main__":
    main()
