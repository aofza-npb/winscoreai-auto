# winscoreai-auto-github/tools/sync_team_mapping.py

import os
import re
import csv
import json
from pathlib import Path

db = None
firebase_admin = None

# ---------------------------
# Firebase init
# ---------------------------
try:
    from firebase_push import db as _db, firebase_admin as _fb
    db = _db
    firebase_admin = _fb
except Exception:
    pass

if not getattr(firebase_admin, "_apps", []):
    import firebase_admin as _fb
    from firebase_admin import credentials, db as _db

    key_json = os.environ.get("FIREBASE_ADMIN_KEY")
    if not key_json:
        raise RuntimeError("FIREBASE_ADMIN_KEY not found in ENV")

    cred = credentials.Certificate(json.loads(key_json))
    _fb.initialize_app(
        cred,
        {
            "databaseURL": "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
        },
    )
    db = _db
    firebase_admin = _fb


# ---------------------------
# Utils
# ---------------------------
def safe_key(key: str) -> str:
    """ทำให้ key ใช้ได้กับ Firebase"""
    if not isinstance(key, str):
        return "unknown"
    key = key.strip()
    if not key:
        return "unknown"
    key = re.sub(r"[.$#[\]/]", "_", key)
    key = key.strip()
    return key or "unknown"


def detect_csv_path() -> Path | None:
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
    mapping: dict[str, str] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return mapping

    header = [c.strip() for c in rows[0]] if rows else []
    body = rows[1:] if rows else []

    if header and all(h != "" for h in header):
        cols = [c.strip() for c in header]

        for r in body:
            row = {cols[i]: r[i] if i < len(r) else "" for i in range(len(cols))}
            if {"eng", "th"}.issubset(cols):
                k, v = row.get("eng", ""), row.get("th", "")
            elif {"eng_name", "th_name"}.issubset(cols):
                k, v = row.get("eng_name", ""), row.get("th_name", "")
            else:
                k, v = r[0], r[1] if len(r) > 1 else ""
            k = safe_key(str(k))
            v = str(v).strip()
            if k and v:
                mapping[k] = v
    else:
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
        print("❌ ไม่พบไฟล์ eng_to_th.csv")
        return

    mapping = load_mapping_from_csv(csv_path)

    # sanitize อีกครั้งกันพลาด
    clean_mapping = {}
    for k, v in mapping.items():
        sk = safe_key(k)
        if sk and v:
            clean_mapping[sk] = v

    if not clean_mapping:
        print("⚠️ mapping ว่าง")
        return

    ref = db.reference("team_mapping/eng_to_th")
    ref.set(clean_mapping)
    print(f"✅ synced {len(clean_mapping)} records from {csv_path}")


if __name__ == "__main__":
    main()
