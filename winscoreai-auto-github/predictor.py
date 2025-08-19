# understat_scraper_auto/predictor.py

import re
import pandas as pd
from datetime import datetime
import pytz
from firebase_push import push_ai_prediction   # <-- ใช้ฟังก์ชันใหม่

def run_prediction():
    # ✅ โหลดข้อมูล
    df = pd.read_csv("understat_scraper_auto/data/win_data.csv")

    df_home = df[df["side"] == "home"]
    df_away = df[df["side"] == "away"]

    predictions = []

    # กำหนดวันที่ไทย (จะใช้เป็นโฟลเดอร์)
    tz = pytz.timezone("Asia/Bangkok")
    date_str = datetime.now(tz).strftime("%Y-%m-%d")

    for home_team in df_home["team"].unique():
        if home_team not in df_away["team"].values:
            continue

        h = df_home[df_home["team"] == home_team].iloc[0]
        a = df_away[df_away["team"] == home_team].iloc[0]

        result = {
            "home": home_team,
            "away": a["team"],
            "lambda_home": round(h["avg_xG"], 2),
            "lambda_away": round(a["avg_xG"], 2),
            "p_over25": round(min(1.0, (h["avg_xG"] + a["avg_xG"]) / 3), 2),
            "p_btts": round(min(1.0, (h["avg_xG"] + a["avg_xG"]) / 4), 2),
            "p_home_hdp_-0.5": round(h["avg_xG"] / (h["avg_xG"] + a["avg_xG"] + 1e-6), 2),
            "pick_main": "-",
            "pick_ou": "-",
            "confidence_pct": 0,
            "edge_pct": 0,
            "stars": 0,
            "reasons": [],
        }

        # 🔍 กฎง่ายๆ (เบื้องต้น)
        if h["avg_xG"] >= 1.6 and a["avg_xGA"] >= 1.4:
            result["pick_ou"] = "Over 2.5"
        elif h["avg_xG"] >= 1.8 and h["avg_xGA"] <= 1.0 and h.get("avg_scored", 0) >= 2.0:
            result["pick_main"] = "ต่อเจ้าบ้าน -0.5"
        elif h["avg_xG"] >= 1.3 and h["avg_xGA"] >= 1.2 and a["avg_xG"] >= 1.2:
            result["pick_ou"] = "BTTS"
        elif h["avg_xG"] < 0.8 and h["avg_xGA"] > 1.6:
            result["pick_main"] = "รองสวนราคา"

        predictions.append(result)

        # ✅ ใช้ fixture_id จาก matches ในอนาคต
        # ตอนนี้ยังไม่มี fixture_id → สร้าง key ชั่วคราว
        fixture_key = f"{home_team.lower().replace(' ', '_')}_{h['latest_date']}"
        fixture_key = re.sub(r'[.#$\[\]/]', '_', fixture_key)

        # ✅ Push เข้า predictions_ai/{date}/{fixture_key}
        push_ai_prediction(result, date_str, fixture_key)

    pd.DataFrame(predictions).to_csv(
        "understat_scraper_auto/data/predict_result.csv", 
        index=False, 
        encoding="utf-8-sig"
    )
    print("✅ วิเคราะห์เสร็จ → predict_result.csv พร้อม push Firebase (predictions_ai/) แล้ว")
