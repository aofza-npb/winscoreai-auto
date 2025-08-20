# tools/sync_team_mapping.py
# -*- coding: utf-8 -*-
import pandas as pd
from firebase_admin import db, initialize_app, credentials
from pathlib import Path

# ✅ init Firebase (ใช้ serviceAccountKey.json ถ้า local, แต่บน GitHub Actions ใช้ env อยู่แล้ว)
try:
    initialize_app()
except ValueError:
    # app ถูก init ไปแล้ว
    pass

def main():
    # หาไฟล์ mapping (ใช้ได้ทั้ง eng,th และ eng_name,th_name)
    candidates = [
        Path("team_mapping/eng_to_th.csv"),
        Path("understat_scraper_auto/team_mapping/eng_to_th.csv"),
        Path("winscoreai-auto-github/team_mapping/eng_to_th.csv"),
    ]
    path = next((p for p in candidates if p.exists()), None)
    if not path:
        print("❌ ไม่พบไฟล์ eng_to_th.csv")
        return

    df = pd.read_csv(path)
    if {"eng", "th"}.issubset(df.columns):
        mapping = {r["eng"]: r["th"] for _, r in df.iterrows() if pd.notna(r["eng"])}
    elif {"eng_name", "th_name"}.issubset(df.columns):
        mapping = {r["eng_name"]: r["th_name"] for _, r in df.iterrows() if pd.notna(r["eng_name"])}
    else:
        print("❌ ไม่พบ header ที่ถูกต้องในไฟล์ CSV")
        return

    # push เข้า Firebase
    ref = db.reference("team_mapping/eng_to_th")
    ref.set(mapping)
    print(f"✅ sync team_mapping เสร็จสิ้น: {len(mapping)} records")

if __name__ == "__main__":
    main()
