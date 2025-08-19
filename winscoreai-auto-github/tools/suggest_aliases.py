# tools/suggest_aliases.py
# -*- coding: utf-8 -*-
import pandas as pd
from firebase_admin import db
from firebase_push import firebase_admin  # ensure initialized

WIN_DATA = "understat_scraper_auto/data/win_data.csv"

def get_match_names():
    root = db.reference("matches").get() or {}
    names = set()
    for _, seasons in (root or {}).items():
        for _, fixtures in (seasons or {}).items():
            for _, obj in (fixtures or {}).items():
                res = (obj or {}).get("results", {})
                h = res.get("teams", {}).get("home", {}).get("name")
                a = res.get("teams", {}).get("away", {}).get("name")
                if isinstance(h, str): names.add(h.strip())
                if isinstance(a, str): names.add(a.strip())
    return names

if __name__ == "__main__":
    df = pd.read_csv(WIN_DATA)
    src_names = set(df["team"].dropna().astype(str).str.strip().unique())
    match_names = get_match_names()

    suggestions = sorted(list(src_names - match_names))
    print("== SUGGEST ALIASES (alias,canonical) ==")
    for n in suggestions[:300]:
        print(f"{n},<canonical>")
