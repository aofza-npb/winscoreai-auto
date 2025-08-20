# tools/sync_team_mapping.py
# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from firebase_push import db  # reuse initialized app

ROOT = Path(__file__).resolve().parents[1]
ENG_PATH = ROOT / "winscoreai-auto-github" / "team_mapping" / "eng_to_th.csv"
ALIAS_PATH = ROOT / "winscoreai-auto-github" / "team_mapping" / "aliases.csv"

def load_csv_to_dict(path, key_col, val_col):
    d = {}
    if path.exists():
        df = pd.read_csv(path)
        for _, r in df.iterrows():
            k = r.get(key_col); v = r.get(val_col)
            if isinstance(k, str) and isinstance(v, str):
                d[k.strip()] = v.strip()
    return d

if __name__ == "__main__":
    eng2th = load_csv_to_dict(ENG_PATH, "eng", "th")
    aliases = load_csv_to_dict(ALIAS_PATH, "alias", "canonical")

    db.reference("team_mapping/eng_to_th").set(eng2th)
    db.reference("team_mapping/aliases").set(aliases)

    print(f"✅ synced team_mapping to Firebase: eng_to_th={len(eng2th)} aliases={len(aliases)}")
