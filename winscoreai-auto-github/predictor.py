# understat_scraper_auto/predictor.py

import re
import pandas as pd
from firebase_push import push_prediction

def run_prediction():
    # ✅ โหลดข้อมูล
    df = pd.read_csv("understat_scraper_auto/data/win_data.csv")

    df_home = df[df["side"] == "home"]
    df_away = df[df["side"] == "away"]

    predictions = []

    for home_team in df_home["team"].unique():
        if home_team not in df_away["team"].values:
            continue

        h = df_home[df_home["team"] == home_team].iloc[0]
        a = df_away[df_away["team"] == home_team].iloc[0]

        result = {
            "team": home_team,
            "latest_date": h["latest_date"],
            "avg_xG": round(h["avg_xG"], 2),
            "avg_xGA": round(h["avg_xGA"], 2),
            "opp_avg_xG": round(a["avg_xG"], 2),
            "opp_avg_xGA": round(a["avg_xGA"], 2),
            "predict": "-"
        }

        # 🔍 วิเคราะห์
        if h["avg_xG"] >= 1.6 and a["avg_xGA"] >= 1.4:
            result["predict"] = "⚽ Over 2.5"
        elif h["avg_xG"] >= 1.8 and h["avg_xGA"] <= 1.0 and h["avg_scored"] >= 2.0:
            result["predict"] = "🔥 ต่อยิงขาด"
        elif h["avg_xG"] >= 1.3 and h["avg_xGA"] >= 1.2 and a["avg_xG"] >= 1.2:
            result["predict"] = "🔁 BTTS (ทั้งคู่ยิง)"
        elif h["avg_xG"] < 0.8 and h["avg_xGA"] > 1.6:
            result["predict"] = "🛡️ รองสวนราคา"

        predictions.append(result)

        match_id = f"{home_team.lower().replace(' ', '_')}_{h['latest_date']}"
        match_id = re.sub(r'[.#$\[\]/]', '_', match_id)

        push_prediction(result, match_id)

    pd.DataFrame(predictions).to_csv("understat_scraper_auto/data/predict_result.csv", index=False, encoding="utf-8-sig")
    print("✅ วิเคราะห์เสร็จ → predict_result.csv พร้อม push Firebase แล้ว")
