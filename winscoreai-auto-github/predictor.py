# understat_scraper_auto/predictor.py
# -*- coding: utf-8 -*-

import re
import unicodedata
from pathlib import Path
from datetime import datetime
import pytz
import pandas as pd

from firebase_admin import db
from firebase_push import push_ai_prediction

# =========================
# Config
# =========================
ROOT = Path(__file__).resolve().parents[1]  # -> winscoreai-auto-github/
MAP_PATH = ROOT / "team_mapping" / "eng_to_th.csv"
ALIAS_PATH = ROOT / "team_mapping" / "aliases.csv"
WIN_DATA_PATH = Path("understat_scraper_auto/data/win_data.csv")
OUT_CSV = Path("understat_scraper_auto/data/predict_result.csv")

# =========================
# Helpers
# =========================
def slugify(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", " ", name)
    name = re.sub(r"[\s\-]+", "_", name)
    return name.strip("_")

def load_eng2th() -> dict:
    d = {}
    if MAP_PATH.exists():
        df = pd.read_csv(MAP_PATH)
        for _, r in df.iterrows():
            eng, th = r.get("eng"), r.get("th")
            if isinstance(eng, str) and isinstance(th, str):
                d[eng.strip()] = th.strip()
    return d

def load_aliases() -> dict:
    d = {}
    if ALIAS_PATH.exists():
        df = pd.read_csv(ALIAS_PATH)
        for _, r in df.iterrows():
            a, c = r.get("alias"), r.get("canonical")
            if isinstance(a, str) and isinstance(c, str):
                d[a.strip().lower()] = c.strip()
    return d

ENG2TH = load_eng2th()
ALIASES = load_aliases()

def normalize_en(name_en: str) -> str:
    if not isinstance(name_en, str):
        return ""
    key = name_en.strip()
    return ALIASES.get(key.lower(), key)

def to_thai(name_en: str) -> str:
    return ENG2TH.get(name_en, name_en)

# =========================
# Build matches index
# =========================
def _extract_results_node(node: dict) -> dict | None:
    """คืน dict ที่มี fields date, teams.home.name, teams.away.name ไม่ว่ามันจะอยู่ที่ results หรือระดับบน"""
    if not isinstance(node, dict):
        return None
    # กรณีมาตรฐาน: มี results
    if isinstance(node.get("results"), dict):
        res = node["results"]
        if isinstance(res.get("teams"), dict) and res.get("date"):
            return res
    # กรณีบาง feed ใส่ไว้บนสุดเลย
    if node.get("date") and isinstance(node.get("teams"), dict):
        return node
    return None

def build_match_index() -> dict:
    """
    index[date_str][home_slug] = (fixture_id, away_slug, full_obj)
    เดินทุกระดับแบบกันพัง: matches -> league -> season -> fixture_id -> {results|top-level}
    """
    root = db.reference("matches").get() or {}
    index: dict[str, dict[str, tuple[str, str, dict]]] = {}

    # บางโปรเจ็คอาจไม่มีแบ่ง league/season ก็รองรับด้วย
    def _walk(fixtures_level):
        if not isinstance(fixtures_level, dict):
            return []
        return fixtures_level.items()

    for _, seasons in _walk(root):
        for _, fixtures in _walk(seasons):
            for fixture_id, node in _walk(fixtures):
                if not isinstance(node, (dict, str)):
                    continue
                # ถ้า node เป็น string/ค่าอื่น ข้าม
                if not isinstance(node, dict):
                    continue
                res = _extract_results_node(node)
                if not res:
                    continue

                date_str = res.get("date")
                teams = res.get("teams", {}) or {}
                home = (teams.get("home") or {}).get("name") or res.get("home_name") or res.get("home")
                away = (teams.get("away") or {}).get("name") or res.get("away_name") or res.get("away")
                if not date_str or not home:
                    continue

                h_en = normalize_en(home)
                a_en = normalize_en(away or "")
                h_slug = slugify(h_en)
                a_slug = slugify(a_en)

                index.setdefault(str(date_str), {})[h_slug] = (str(fixture_id), a_slug, node)

    # เผื่อรูปแบบที่ matches ไม่มีชั้น league/season (rare)
    if not index and isinstance(root, dict):
        for fixture_id, node in root.items():
            if not isinstance(node, dict):
                continue
            res = _extract_results_node(node)
            if not res:
                continue
            date_str = res.get("date")
            teams = res.get("teams", {}) or {}
            home = (teams.get("home") or {}).get("name")
            away = (teams.get("away") or {}).get("name")
            if not date_str or not home:
                continue
            h_en = normalize_en(home); a_en = normalize_en(away or "")
            h_slug = slugify(h_en); a_slug = slugify(a_en)
            index.setdefault(str(date_str), {})[h_slug] = (str(fixture_id), a_slug, node)

    return index


def pick_fixture_id(match_index: dict, date_str: str, home_en: str, away_en: str | None) -> str | None:
    if date_str not in match_index:
        return None
    h_slug = slugify(normalize_en(home_en))
    a_slug = slugify(normalize_en(away_en)) if away_en else None
    tup = match_index[date_str].get(h_slug)
    if not tup:
        return None
    fixture_id, away_slug_in_db, _ = tup
    if a_slug and away_slug_in_db and a_slug != away_slug_in_db:
        return None
    return fixture_id

# =========================
# Simple rule model (MVP)
# =========================
def simple_rules(h_row: pd.Series, a_row: pd.Series):
    lam_h = float(round(h_row["avg_xG"], 2))
    lam_a = float(round(a_row["avg_xG"], 2))
    p_over25 = round(min(1.0, (h_row["avg_xG"] + a_row["avg_xG"]) / 3), 2)
    p_btts = round(min(1.0, (h_row["avg_xG"] + a_row["avg_xG"]) / 4), 2)
    pick_main, pick_ou = "-", "-"
    if h_row["avg_xG"] >= 1.6 and a_row["avg_xGA"] >= 1.4:
        pick_ou = "Over 2.5"
    elif h_row["avg_xG"] >= 1.8 and h_row["avg_xGA"] <= 1.0 and (h_row.get("avg_scored", 0) >= 2.0):
        pick_main = "ต่อเจ้าบ้าน -0.5"
    elif h_row["avg_xG"] >= 1.3 and h_row["avg_xGA"] >= 1.2 and a_row["avg_xG"] >= 1.2:
        pick_ou = "BTTS"
    elif h_row["avg_xG"] < 0.8 and h_row["avg_xGA"] > 1.6:
        pick_main = "รองสวนราคา"
    return lam_h, lam_a, p_over25, p_btts, pick_main, pick_ou

# =========================
# MAIN
# =========================
def run_prediction():
    df = pd.read_csv(WIN_DATA_PATH)
    df_home = df[df["side"] == "home"]
    df_away = df[df["side"] == "away"]

    match_index = build_match_index()

    tz = pytz.timezone("Asia/Bangkok")
    today_str = datetime.now(tz).strftime("%Y-%m-%d")

    rows_out = []

    for home_en in df_home["team"].unique():
        if home_en not in df_away["team"].values:
            continue

        h = df_home[df_home["team"] == home_en].iloc[0]
        a = df_away[df_away["team"] == home_en].iloc[0]

        home_en_norm = normalize_en(home_en)
        away_en_norm = normalize_en(a["team"])
        home_th = to_thai(home_en_norm)
        away_th = to_thai(away_en_norm)
        latest_date = str(h["latest_date"])

        lam_h, lam_a, p_over25, p_btts, pick_main, pick_ou = simple_rules(h, a)
        p_hdp = round(lam_h / (lam_h + lam_a + 1e-6), 2)

        ai_data = {
            "home": home_en_norm,
            "home_th": home_th,
            "away": away_en_norm,
            "away_th": away_th,
            "lambda_home": lam_h,
            "lambda_away": lam_a,
            "p_over25": p_over25,
            "p_btts": p_btts,
            "p_home_hdp_-0.5": p_hdp,
            "pick_main": pick_main,
            "pick_ou": pick_ou,
            "confidence_pct": 0,
            "edge_pct": 0,
            "stars": 0,
            "reasons": [],
        }

        fixture_id = pick_fixture_id(match_index, latest_date, home_en_norm, away_en_norm)

        if not fixture_id:
            fixture_key = f"{slugify(home_en_norm)}_{latest_date}"
            push_ai_prediction(ai_data, date_str=today_str, fixture_id=fixture_key)
            fixture_out = fixture_key
        else:
            push_ai_prediction(ai_data, date_str=today_str, fixture_id=str(fixture_id))
            fixture_out = str(fixture_id)

        rows_out.append({
            "date": latest_date,
            "fixture": fixture_out,
            **ai_data,
        })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_out).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print("✅ วิเคราะห์และเขียน Firebase เสร็จ (predictions_ai/)")
