# understat_scraper_auto/predictor.py

import re
import unicodedata
from datetime import datetime
import pytz
import pandas as pd

# Firebase: ใช้การ init จากไฟล์ firebase_push.py (อย่าลบ)
from firebase_admin import db
from firebase_push import push_ai_prediction


# ---------- utils ----------
def slugify(name: str) -> str:
    """
    แปลงชื่อทีมให้เทียบกันได้: lowercase, ตัดวรรณยุกต์/สัญลักษณ์, เว้นวรรค -> _
    """
    if not isinstance(name, str):
        return ""
    # Normalize (ตัดเครื่องหมายกำกับเสียง/วรรณยุกต์ออกได้ดีทั้ง EN/TH)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", " ", name)       # เก็บเฉพาะตัวอักษร/ตัวเลข/ขีด/ขีดล่าง/เว้นวรรค
    name = re.sub(r"[\s\-]+", "_", name)        # ช่องว่าง/ขีดหลายตัว -> _
    return name.strip("_")


def build_match_index() -> dict:
    root = db.reference("matches").get() or {}
    index = {}

    for league_id, seasons in (root or {}).items():
        for season, fixtures in (seasons or {}).items():
            for fixture_id, obj in (fixtures or {}).items():
                results = obj.get("results", {})
                date_str = results.get("date")
                if not date_str:
                    continue

                h_name = results.get("teams", {}).get("home", {}).get("name")
                a_name = results.get("teams", {}).get("away", {}).get("name")

                h_slug = slugify(h_name)
                a_slug = slugify(a_name)

                index.setdefault(date_str, {})[h_slug] = (str(fixture_id), a_slug, obj)
    return index



def pick_fixture_id(match_index: dict, date_str: str, home_team: str, away_team: str | None) -> str | None:
    """
    พยายามหา fixture_id ด้วย date + home_slug (+ ตรวจ away ถ้ามี)
    """
    if date_str not in match_index:
        return None
    h_slug = slugify(home_team)
    a_slug = slugify(away_team) if away_team else None
    tup = match_index[date_str].get(h_slug)
    if not tup:
        return None
    fixture_id, away_slug_in_db, _ = tup
    if a_slug and away_slug_in_db and a_slug != away_slug_in_db:
        # ถ้า away ไม่ตรง ให้ถือว่ายังไม่ชัวร์ -> None (จะ fallback ทีหลัง)
        return None
    return fixture_id


# ---------- main ----------
def run_prediction():
    # 1) โหลดข้อมูล feature ที่คุณสร้างไว้
    df = pd.read_csv("understat_scraper_auto/data/win_data.csv")
    df_home = df[df["side"] == "home"]
    df_away = df[df["side"] == "away"]

    # 2) สร้างดัชนี fixtures จาก Firebase
    match_index = build_match_index()

    # 3) วันที่ (ใช้เป็นโฟลเดอร์ผล AI)
    tz = pytz.timezone("Asia/Bangkok")
    today_str = datetime.now(tz).strftime("%Y-%m-%d")

    rows_out = []

    for home_team in df_home["team"].unique():
        if home_team not in df_away["team"].values:
            continue

        h = df_home[df_home["team"] == home_team].iloc[0]
        a = df_away[df_away["team"] == home_team].iloc[0]
        latest_date = str(h["latest_date"])

        # --------- กฎง่าย ๆ เพื่อเดโม (คุณค่อยแทนด้วยผลโมเดลจริงภายหลัง) ---------
        lambda_home = float(round(h["avg_xG"], 2))
        lambda_away = float(round(a["avg_xG"], 2))
        p_over25 = round(min(1.0, (h["avg_xG"] + a["avg_xG"]) / 3), 2)
        p_btts = round(min(1.0, (h["avg_xG"] + a["avg_xG"]) / 4), 2)
        p_hdp = round(lambda_home / (lambda_home + lambda_away + 1e-6), 2)

        pick_main, pick_ou = "-", "-"
        if h["avg_xG"] >= 1.6 and a["avg_xGA"] >= 1.4:
            pick_ou = "Over 2.5"
        elif h["avg_xG"] >= 1.8 and h["avg_xGA"] <= 1.0 and (h.get("avg_scored", 0) >= 2.0):
            pick_main = "ต่อเจ้าบ้าน -0.5"
        elif h["avg_xG"] >= 1.3 and h["avg_xGA"] >= 1.2 and a["avg_xG"] >= 1.2:
            pick_ou = "BTTS"
        elif h["avg_xG"] < 0.8 and h["avg_xGA"] > 1.6:
            pick_main = "รองสวนราคา"
        # -------------------------------------------------------------------------

        ai_data = {
            "home": home_team,
            "away": a["team"],
            "lambda_home": lambda_home,
            "lambda_away": lambda_away,
            "p_over25": p_over25,
            "p_btts": p_btts,
            "p_home_hdp_-0.5": p_hdp,
            "pick_main": pick_main,
            "pick_ou": pick_ou,
            # คุณจะคำนวน 3 ค่าเหล่านี้จากโมเดลจริง/odds ภายหลัง
            "confidence_pct": 0,
            "edge_pct": 0,
            "stars": 0,
            "reasons": [],
        }

        # 4) หา fixture_id จาก matches ด้วย date + home(+away)
        fixture_id = pick_fixture_id(
            match_index=match_index,
            date_str=latest_date,          # ใช้วันที่จากข้อมูลทีม (ควรตรงกับ matches.date)
            home_team=home_team,
            away_team=a["team"],
        )

        # 5) ถ้าไม่เจอ ให้ fallback เป็น fixture_key ชั่วคราว
        if not fixture_id:
            fixture_key = f"{slugify(home_team)}_{latest_date}"
            push_ai_prediction(ai_data, date_str=today_str, fixture_id=fixture_key)
        else:
            push_ai_prediction(ai_data, date_str=today_str, fixture_id=str(fixture_id))

        rows_out.append({
            "date": latest_date,
            "home": home_team,
            "away": a["team"],
            "fixture_id": fixture_id or f"{slugify(home_team)}_{latest_date}",
            **ai_data,
        })

    # 6) เขียนไฟล์สรุป CSV สำหรับตรวจสอบ
    pd.DataFrame(rows_out).to_csv(
        "understat_scraper_auto/data/predict_result.csv",
        index=False,
        encoding="utf-8-sig"
    )
    print("✅ วิเคราะห์และ push Firebase เสร็จ (predictions_ai/)")
