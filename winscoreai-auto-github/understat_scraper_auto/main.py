import os
import pandas as pd
import json
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
import schedule
import time

LEAGUE_SCHEDULE = ["EPL", "La Liga", "Serie A", "Bundesliga", "Ligue 1"]
LEAGUE_MAPPING = {
    "EPL": "EPL",
    "La Liga": "La_liga",
    "Serie A": "Serie_A",
    "Bundesliga": "Bundesliga",
    "Ligue 1": "Ligue_1"
}
DATA_DIR = "understat_scraper_auto/data"
MAPPING_FILE = "understat_scraper_auto/team_mapping/eng_to_th.csv"
START_YEAR = 2014
CURRENT_YEAR = datetime.now().year
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs("team_mapping", exist_ok=True)
os.makedirs(os.path.dirname(MAPPING_FILE), exist_ok=True)
# Load or initialize team mapping
if os.path.exists(MAPPING_FILE):
    df_map = pd.read_csv(MAPPING_FILE)
    ENG2TH = dict(zip(df_map["eng"], df_map["th"]))
else:
    ENG2TH = {}

def save_team_mapping(new_names):
    global ENG2TH
    existing = set(ENG2TH.keys())
    to_add = sorted(set(new_names) - existing)
    if to_add:
        df_add = pd.DataFrame({"eng": to_add, "th": to_add})
        df_existing = pd.DataFrame(ENG2TH.items(), columns=["eng", "th"])
        df_all = pd.concat([df_existing, df_add], ignore_index=True)
        df_all.to_csv(MAPPING_FILE, index=False, encoding="utf-8-sig")
        ENG2TH = dict(zip(df_all["eng"], df_all["th"]))

def extract_xg(team_obj):
    try:
        return float(team_obj.get("xG", None))
    except:
        return None

def fetch_league_data(league):
    league_code = LEAGUE_MAPPING[league]
    csv_path = f"{DATA_DIR}/understat_{league_code.lower()}.csv"
    if os.path.exists(csv_path):
        print(f"⏭ ข้าม {league} (มีไฟล์อยู่แล้ว)")
        return
    print(f"📥 กำลังดึงข้อมูล {league} ...")
    all_data = []
    all_team_names = set()

    for year in range(START_YEAR, CURRENT_YEAR + 1):
        url = f"https://understat.com/league/{league_code}/{year}"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        script = soup.find("script", text=lambda t: t and "teamsData" in t)
        pattern = re.search(r"var\s+teamsData\s+=\s+JSON\.parse\('(.*?)'\);", script.text)
        if not pattern:
            raise Exception(f"❌ ไม่พบข้อมูล teamsData จาก {url}")
        json_str = pattern.group(1).encode("utf8").decode("unicode_escape")
        teams_data = json.loads(json_str)

        for team_id, team_obj in teams_data.items():
            team_name_en = team_obj["title"]
            all_team_names.add(team_name_en)
            team_name_th = ENG2TH.get(team_name_en, team_name_en)
            for match in team_obj["history"]:
                row = {
                    "date": parser.parse(match["date"]).strftime("%#d %b %Y"),
                    "season": year,
                    "team": team_name_th,
                    "xG": extract_xg(match),
                    "xGA": float(match.get("xGA", 0)),
                    "scored": int(match["scored"]),
                    "missed": int(match["missed"]),
                    "result": match["result"],
                    "npxG": float(match.get("npxG", 0)),
                    "deep": int(match.get("deep", 0)),
                    "ppda": float(match.get("ppda", {}).get("att", 0)),
                    "xpts": float(match.get("xpts", 0)),
                    "h_a": match["h_a"]
                }

               
                all_data.append(row)

    save_team_mapping(all_team_names)
    df = pd.DataFrame(all_data)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ ดึงข้อมูล {league} ครบแล้ว")

def job():
    today_index = datetime.today().weekday() % len(LEAGUE_SCHEDULE)
    league_today = LEAGUE_SCHEDULE[today_index]
    fetch_league_data(league_today)
    print("🎯 ระบบอัปเดตเรียบร้อย")

if __name__ == "__main__":
    job()  # Run once immediately
    schedule.every().day.at("06:00").do(job)
    while True:
        schedule.run_pending()
        time.sleep(60)
          
