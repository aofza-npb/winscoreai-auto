# -*- coding: utf-8 -*-
"""
af_today_odds.py — Pull odds for today's fixtures (and optional next days) for target leagues,
covering 3 markets: 1X2, Over/Under, Handicap. Writes:
  - JSON: live_odds/odds_full_YYYYMMDD_YYYYMMDD.json
  - CSV (flat): live_odds/odds_flat_all_YYYYMMDD_YYYYMMDD.csv
Args:
  --date YYYY-MM-DD (default=UTC today)
  --days N (default=1)  # span date..date+days-1
  --allow PATH          # allowlist file with league_id (integers)
  --bookmaker INT       # if set, restrict to one bookmaker id (e.g., 6 = Bet365). default=ALL
  --outdir DIR          # default=live_odds
Notes:
  - Fallback strategy if /fixtures?date=... returns none for some day:
      a) try /fixtures?from=..&to=..&timezone=Asia/Bangkok
      b) try per-league /fixtures?league=..&season=..&next=.. then filter by date window
  - Requires env: API_FOOTBALL_KEY (and optionally API_FOOTBALL_VENDOR=apisports|rapidapi)
"""

import os
import csv
import re
import json
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

import requests

# ---------- Config / ENV ----------

load_dotenv()  # โหลดค่าจาก .env

API_KEY = os.getenv("API_FOOTBALL_KEY")
VENDOR = os.getenv("API_FOOTBALL_VENDOR", "apisports")

if VENDOR == "rapidapi":
    BASE = "https://api-football-v1.p.rapidapi.com/v3"
    HEAD = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
else:
    BASE = "https://v3.football.api-sports.io"
    HEAD = {"x-apisports-key": API_KEY}

COMMON_STATUS_KEEP = {"Not Started", "Time to be defined", "Scheduled"}  # ← เพิ่ม Scheduled

# ---------- HTTP helper ----------
def req_get(path, params, what="", retry=3, wait=1.2):
    url = f"{BASE}/{path}"
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEAD, params=params, timeout=30)
            if r.status_code == 200:
                j = r.json()
                return j.get("response", []), j.get("errors", {}), j.get("results", 0)
        except requests.RequestException:
            pass
        time.sleep(wait)
    raise RuntimeError(f"GET {what or path} failed after retries")

# ---------- parsing odds ----------
def parse_1x2(bet):
    rec = {"home": "", "draw": "", "away": ""}
    for v in bet.get("values", []):
        val = (v.get("value") or "").strip().lower()
        odd = v.get("odd") or ""
        if val in ("home", "1", "1 (home)"):
            rec["home"] = odd
        elif val in ("draw", "x"):
            rec["draw"] = odd
        elif val in ("away", "2", "2 (away)"):
            rec["away"] = odd
    return rec

def parse_ou(bet):
    # return mapping line -> {"over": x, "under": y}
    res = {}
    for v in bet.get("values", []):
        val = (v.get("value") or "").strip()
        odd = v.get("odd") or ""
        m = re.search(r"([Oo]ver|[Uu]nder)\s*([+-]?\d+(?:\.\d+)?)", val)
        if not m:
            m2 = re.search(r"([+-]?\d+(?:\.\d+)?)\s*(Over|Under)", val, flags=re.I)
            if not m2:
                continue
            side = m2.group(2).lower()
            line = m2.group(1)
        else:
            side = m.group(1).lower()
            line = m.group(2)
        d = res.setdefault(line, {"over": "", "under": ""})
        if "over" in side:
            d["over"] = odd
        else:
            d["under"] = odd
    return res

def parse_hcp(bet):
    # return mapping line -> {"home": x, "away": y}
    res = {}
    for v in bet.get("values", []):
        val = (v.get("value") or "").strip()
        odd = v.get("odd") or ""
        side = "home" if re.search(r"\b(home|^1\b)", val, re.I) else ("away" if re.search(r"\b(away|^2\b)", val, re.I) else None)
        line = None
        for tok in re.findall(r"[+-]?\d+(?:\.\d+)?", val.replace(":", " ")):
            try:
                float(tok)
                line = tok
                break
            except Exception:
                pass
        if side and line is not None:
            d = res.setdefault(line, {"home": "", "away": ""})
            d[side] = odd
    return res

def extract_markets(odds_payload):
    """Return bookmakers dict: {bm_id: {"1x2":{...}, "ou":{line:{...}}, "hcp":{line:{...}}}}"""
    books = {}
    for entry in odds_payload:
        for bm in entry.get("bookmakers", []):
            bm_id = bm.get("id")
            acc = books.setdefault(str(bm_id), {"1x2": {}, "ou": {}, "hcp": {}})
            for bet in bm.get("bets", []):
                name = (bet.get("name") or "").lower()
                if "match winner" in name or name.strip() in ("1x2", "winner", "win/lose", "win-draw-win"):
                    acc["1x2"] = parse_1x2(bet)
                elif "over/under" in name or "total goals" in name or "goals over" in name:
                    acc["ou"] = parse_ou(bet)
                elif "handicap" in name:
                    acc["hcp"] = parse_hcp(bet)
    return books

# ---------- allowlist ----------
def read_allowlist(path):
    lids = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            p = s.split()
            if p[0].isdigit():
                lids.append(int(p[0]))
    return set(lids)

# ---------- fixtures fetch with fallbacks ----------
def season_guess(d_utc):
    return d_utc.year if d_utc.month >= 7 else d_utc.year - 1

def fetch_fixtures_for_day(target_date, lids, tz="Asia/Bangkok", per_league_next=20):
    ds = target_date.strftime("%Y-%m-%d")

    # 1) direct date
    fx, err, _ = req_get("fixtures", {"date": ds}, what=f"fixtures?date={ds}")
    fx = [x for x in fx if int(x.get("league", {}).get("id", -1)) in lids]
    if fx:
        return fx

    # 2) window from/to with timezone
    d0 = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    d1 = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    params = {"from": d0, "to": d1, "timezone": tz}
    if VENDOR == "rapidapi":
        params = {"from": d0, "to": d1, "timezone": tz}  # same keys for RapidAPI v3
    fx, err, _ = req_get("fixtures", params, what=f"fixtures window {d0}..{d1}")
    fx = [x for x in fx if int(x.get("league", {}).get("id", -1)) in lids
          and (x.get("fixture", {}).get("date", "") or "").startswith(ds)]
    if fx:
        return fx

    # 3) per-league next (season-aware), then filter target day
    out = []
    sea = season_guess(target_date)
    for lid in lids:
        f2, _, _ = req_get("fixtures", {"league": lid, "season": sea, "next": per_league_next},
                          what=f"fixtures league={lid} next")
        for x in f2:
            if (x.get("fixture", {}).get("date", "") or "").startswith(ds):
                out.append(x)
    return out

def status_long(rec):
    return rec.get("fixture", {}).get("status", {}).get("long")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"), help="YYYY-MM-DD (UTC)")
    ap.add_argument("--days", type=int, default=1, help="how many days (today..today+days-1)")
    ap.add_argument("--allow", default="allowlist_ALL.txt", help="allowlist file (1st col = league_id)")
    ap.add_argument("--bookmaker", type=int, default=0, help="0 or omit = ALL; e.g., 6=Bet365")
    ap.add_argument("--outdir", default="live_odds", help="output folder")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    lids = read_allowlist(args.allow)
    start = datetime.strptime(args.date, "%Y-%m-%d").date()
    end = start + timedelta(days=args.days-1)

    print(f"ดึง fixtures+odds: {start} → {end} | leagues={len(lids)} | bookmaker={args.bookmaker or 'ALL'} | vendor={VENDOR}")

    all_fixtures = []   # for JSON
    flat_rows = []      # for CSV

    for d in (start + timedelta(days=i) for i in range(args.days)):
        ds = d.strftime("%Y-%m-%d")
        fixtures = fetch_fixtures_for_day(d, lids)
        # keep only not-started
        def keep_before_ko(rec, grace_seconds=900):
          ts = rec.get("fixture", {}).get("timestamp")
        # เก็บเฉพาะแมตช์ที่ยังไม่เริ่ม (เผื่อเวลา 15 นาที)
          return isinstance(ts, int) and ts >= int(time.time()) - grace_seconds

        fixtures = [x for x in fixtures if keep_before_ko(x)]

        print(f"• {ds}: fixtures before KO = {len(fixtures)}")
        print("Raw fixtures fetched:", len(fixtures))
        print("Sample:", fixtures[:3])

        for f in fixtures:
            fid = int(f["fixture"]["id"])
            lid = int(f["league"]["id"])
            season = int(f["league"]["season"])
            home = f["teams"]["home"]["name"]
            away = f["teams"]["away"]["name"]
            params = {"fixture": fid}
            if args.bookmaker:
                params["bookmaker"] = args.bookmaker

            odds, _, _ = req_get("odds", params, what=f"odds fixture={fid}")
            books = extract_markets(odds)

            rec = {
                "date": ds,
                "season": season,
                "league_id": lid,
                "fixture_id": fid,
                "kickoff_ts": f["fixture"]["timestamp"],  # ← เพิ่ม
                "home": home,
                "away": away,
                "bookmakers": books
            }
            all_fixtures.append(rec)

            # also build flat rows for CSV
            for bm_id, mk in books.items():
                # 1X2
                if mk.get("1x2"):
                    one = mk["1x2"]
                    for side in ("home", "draw", "away"):
                        if one.get(side):
                            flat_rows.append([season, ds, lid, fid, home, away, "1x2", "", side, one[side], bm_id])
                # OU
                for line, v in (mk.get("ou") or {}).items():
                    if v.get("over"):
                        flat_rows.append([season, ds, lid, fid, home, away, "ou", line, "over", v["over"], bm_id])
                    if v.get("under"):
                        flat_rows.append([season, ds, lid, fid, home, away, "ou", line, "under", v["under"], bm_id])
                # HCP
                for line, v in (mk.get("hcp") or {}).items():
                    if v.get("home"):
                        flat_rows.append([season, ds, lid, fid, home, away, "hcp", line, "home", v["home"], bm_id])
                    if v.get("away"):
                        flat_rows.append([season, ds, lid, fid, home, away, "hcp", line, "away", v["away"], bm_id])

            time.sleep(0.2)  # be nice to rate limit

    # write JSON
    jpath = outdir / f"odds_full_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.json"
    with open(jpath, "w", encoding="utf-8") as w:
        json.dump({"fixtures": all_fixtures, "meta": {
            "date_from": start.strftime("%Y-%m-%d"),
            "date_to": end.strftime("%Y-%m-%d"),
            "vendor": VENDOR,
            "bookmaker": args.bookmaker or "ALL",
            "leagues": len(lids),
        }}, w, ensure_ascii=False)

    # write CSV (flat)
    cpath = outdir / f"odds_flat_all_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.csv"
    with open(cpath, "w", newline="", encoding="utf-8") as w:
        wr = csv.writer(w)
        wr.writerow(["season","date","league_id","fixture_id","home","away","market","line","side","odd","bookmaker_id"])
        wr.writerows(flat_rows)

    print(f"✅ JSON: {jpath} | fixtures={len(all_fixtures)}")
    print(f"✅ CSV : {cpath} | rows={len(flat_rows)}")
    if all_fixtures[:2]:
        print("ตัวอย่าง 1–2 fixtures:")
        for r in all_fixtures[:2]:
            print("  ", r["league_id"], r["fixture_id"], r["home"], "vs", r["away"], "| bm:", len(r["bookmakers"]))
if __name__ == "__main__":
    main()

