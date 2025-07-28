# understat_scraper_auto/win_data.py

import pandas as pd
import os
from datetime import datetime

def generate_win_data():
    DATA_DIR = "understat_scraper_auto/data"
    OUTPUT_FILE = os.path.join(DATA_DIR, "win_data.csv")

    csv_files = [f for f in os.listdir(DATA_DIR) if f.startswith("understat_") and f.endswith(".csv")]
    df_all = pd.concat([pd.read_csv(os.path.join(DATA_DIR, f)) for f in csv_files])

    try:
        df_all["date"] = pd.to_datetime(df_all["date"], format="%d %b %Y")
    except:
        df_all["date"] = pd.to_datetime(df_all["date"])

    df_all = df_all.sort_values(by="date", ascending=False)

    all_rows = []
    for is_home in ["h", "a"]:
        df_side = df_all[df_all["h_a"] == is_home]
        for team in df_side["team"].unique():
            df_team = df_side[df_side["team"] == team].copy()
            df_team = df_team.sort_values(by="date", ascending=False)

            for i in range(len(df_team)):
                recent = df_team.iloc[i+1:i+6]
                if len(recent) < 3:
                    continue

                row = {
                    "team": team,
                    "latest_date": df_team.iloc[i]["date"].strftime("%Y-%m-%d"),
                    "side": "home" if is_home == "h" else "away",
                    "avg_xG": recent["xG"].mean(),
                    "avg_xGA": recent["xGA"].mean(),
                    "avg_scored": recent["scored"].mean(),
                    "avg_missed": recent["missed"].mean(),
                    "avg_xpts": recent["xpts"].mean(),
                    "games_count": len(recent)
                }
                all_rows.append(row)

    win_data = pd.DataFrame(all_rows)
    win_data.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ สร้างไฟล์ win_data.csv เรียบร้อยแล้ว → {OUTPUT_FILE}")
