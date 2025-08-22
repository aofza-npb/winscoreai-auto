# scripts/af_results.py
# -*- coding: utf-8 -*-
"""
Pull finished results (HT/FT) + xG (best-effort) and store to JSON.

Outputs:
  results_full_YYYYMMDD_YYYYMMDD.json

Args:
  --from YYYY-MM-DD  start date (UTC) inclusive
  --to   YYYY-MM-DD  end   date (UTC) inclusive
  --days N           instead of --from/--to, use [today-N+1..today] UTC
  --allow PATH       allowlist leagues (default allowlist_ALL.txt)
  --outdir DIR       output dir (default results)
  --no-xg            skip xG fetching
Env:
  API_FOOTBALL_KEY
  API_FOOTBALL_VENDOR=apisports|rapidapi  (default apisports)
"""

import os, re, json, time, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import requests

load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")
VENDOR = os.getenv("API_FOOTBALL_VENDOR", "apisports")

if VENDOR == "rapidapi":
    BASE = "https://api-football-v1.p.rapidapi.com/v3"
    HEAD = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
else:
    BASE = "https://v3.football.api-sports.io"
    HEAD = {"x-apisports-key": API_KEY}

KEEP_STATUS = {"Match Finished", "After Pen." , "After ET"}  # ครอบคลุม FT หลายแบบ

def req_get(path, params, what="", retry=3, wait=1.0):
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
    raise RuntimeError(f"GET {what or path} failed")

def read_allow(path):
    s=set()
    with open(path,encoding="utf-8") as f:
        for line in f:
            t=line.strip()
            if not t or t.startswith("#"): continue
            p=t.split()
            if p[0].isdigit(): s.add(int(p[0]))
    return s

def season_guess(d):
    return d.year if d.month>=7 else d.year-1

def status_long(rec):
    return rec.get("fixture",{}).get("status",{}).get("long")

def winner_code(rec):
    w = rec.get("teams",{})
    # API returns "winner": True/False under each team
    if w.get("home",{}).get("winner") is True:  return "home"
    if w.get("away",{}).get("winner") is True:  return "away"
    return "draw"

def goals_ht(rec):
    s = rec.get("score",{}).get("halftime",{})
    return s.get("home"), s.get("away")

def goals_ft(rec):
    g = rec.get("goals",{})
    return g.get("home"), g.get("away")
# ==== เพิ่มด้านบนใกล้ๆ fetch_xg() ====
def _int(v):
    try: return int(str(v).split()[0])
    except: return 0

def estimate_xg_from_stats(team_stat_obj):
    """
    team_stat_obj = item ใต้ response[] ของ /fixtures/statistics สำหรับทีมหนึ่ง
    คืนค่า xG แบบประมาณ (float) หรือ None ถ้าข้อมูลไม่พอ
    """
    if not team_stat_obj:
        return None
    stats = team_stat_obj.get("statistics") or []
    # ดึงค่าเป็นดิกตามชื่อ
    kv = { (s.get("type") or "").lower(): s.get("value") for s in stats }

    sog   = _int(kv.get("shots on goal"))
    tso   = _int(kv.get("total shots"))
    inbx  = _int(kv.get("shots insidebox") or kv.get("shots inside box"))
    outbx = _int(kv.get("shots outsidebox") or kv.get("shots outside box"))
    big   = _int(kv.get("big chances"))
    pens  = _int(kv.get("penalties") or kv.get("penalty") or kv.get("penalties scored"))

    # ถ้าไม่มีข้อมูลยิงเลย ประมาณไม่ได้
    if (sog + tso + inbx + outbx + big + pens) == 0:
        return None

    # น้ำหนักปรับได้ผ่าน ENV (มีค่า default ที่สมเหตุสมผล)
    W_INBOX      = float(os.getenv("XG_W_INBOX",      "0.13"))  # ยิงในกรอบเฉลี่ย ~0.12–0.15
    W_OUTBOX     = float(os.getenv("XG_W_OUTBOX",     "0.03"))  # นอกกรอบเฉลี่ย ~0.02–0.04
    W_SOG_BONUS  = float(os.getenv("XG_W_SOG_BONUS",  "0.02"))  # โบนัสความคม (on target)
    W_BIG        = float(os.getenv("XG_W_BIGCHANCE",  "0.25"))  # big chance เพิ่มค่าน่าจะเป็น
    W_PEN        = float(os.getenv("XG_W_PEN",        "0.76"))  # ค่าคงที่ของจุดโทษ

    # ฐานจากตำแหน่งยิง (ถ้า in/out ไม่ครบ ให้ประมาณจาก total shots)
    if inbx == 0 and outbx == 0 and tso > 0:
        # เดา: 65% ยิงในกรอบ
        inbx  = int(round(tso * 0.65))
        outbx = max(0, tso - inbx)

    base = inbx * W_INBOX + outbx * W_OUTBOX

    # โบนัสความคม: จำนวน SoG ที่เกินสัดส่วนจาก in-box (กัน double-count แบบคร่าว ๆ)
    expected_sog_from_inbox = int(round(inbx * 0.6))
    sog_bonus = max(0, sog - expected_sog_from_inbox) * W_SOG_BONUS

    # big chance และ penalty
    add_big = big * W_BIG
    add_pen = pens * W_PEN

    xg_est = base + sog_bonus + add_big + add_pen
    return round(float(xg_est), 3)

def fetch_xg_or_estimate(fixture_id):
    """
    พยายามอ่าน xG จริงจาก /fixtures/statistics
    ถ้าไม่มี ให้คำนวณประมาณ xG_est เองต่อทีม (home, away)
    """
    try:
        stats, _, _ = req_get("fixtures/statistics", {"fixture": fixture_id}, what=f"xg {fixture_id}")
        # พยายามอ่าน xG จริงก่อน
        def read_true_xg(obj):
            for v in (obj.get("statistics") or []):
                nm = (v.get("type") or "").lower()
                if "expected goals" in nm or nm in ("xg", "x-g", "x_g"):
                    try: return float(v.get("value"))
                    except:
                        try: return float(str(v.get("value")))
                        except: return None
            return None

        h_true = read_true_xg(stats[0]) if len(stats) >= 1 else None
        a_true = read_true_xg(stats[1]) if len(stats) >= 2 else None

        if h_true is not None or a_true is not None:
            return h_true, a_true

        # ไม่มีของจริง → ประมาณเอง
        h_est = estimate_xg_from_stats(stats[0]) if len(stats) >= 1 else None
        a_est = estimate_xg_from_stats(stats[1]) if len(stats) >= 2 else None
        return h_est, a_est
    except Exception:
        return None, None

def fetch_xg(fixture_id):
    # best-effort: look in /fixtures/statistics for "Expected Goals" or "xG"
    try:
        stats,_,_ = req_get("fixtures/statistics", {"fixture": fixture_id}, what=f"xg {fixture_id}")
        h_xg=a_xg=None
        for team_stat in stats:
            team = team_stat.get("team",{})
            vals = team_stat.get("statistics",[]) or []
            # search keys that look like expected goals
            found=None
            for v in vals:
                name = (v.get("type") or "").lower()
                if "expected goals" in name or name.strip() in ("xg","x-g","x_g"):
                    found = v.get("value")
                    break
            if found is None:
                continue
            try:
                fx = float(found)
            except:
                # strings like "1.23" ok, others None
                try: fx = float(str(found))
                except: fx=None
            if fx is None: continue
            if team.get("name","").strip():
                # we don't know H/A from this list order → infer later
                pass
        # Some tenants provide two items ordered [home,away]
        if len(stats)==2:
            def read_one(obj):
                for v in obj.get("statistics",[]) or []:
                    nm=(v.get("type") or "").lower()
                    if "expected goals" in nm or nm in ("xg","x-g","x_g"):
                        try: return float(v.get("value"))
                        except: 
                            try: return float(str(v.get("value")))
                            except: return None
                return None
            h_xg = read_one(stats[0])
            a_xg = read_one(stats[1])
        return h_xg, a_xg
    except Exception:
        return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", help="YYYY-MM-DD UTC")
    ap.add_argument("--to", dest="date_to", help="YYYY-MM-DD UTC")
    ap.add_argument("--days", type=int, default=0, help="use [today-days+1..today] UTC")
    ap.add_argument("--allow", default="allowlist_ALL.txt")
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--no-xg", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    lids = read_allow(args.allow)

    if args.days and not (args.date_from or args.date_to):
        end = datetime.utcnow().date()
        start = end - timedelta(days=args.days-1)
    else:
        if not (args.date_from and args.date_to):
            raise SystemExit("use --days N OR --from A --to B")
        start = datetime.strptime(args.date_from,"%Y-%m-%d").date()
        end   = datetime.strptime(args.date_to  ,"%Y-%m-%d").date()

    print(f"ดึงผลแข่งจบแล้ว: {start}..{end} | leagues={len(lids)} | vendor={VENDOR}")

    all_recs=[]
    # fetch per-day (เร็วและง่าย)
    day = start
    while day <= end:
        ds = day.strftime("%Y-%m-%d")
        fx,_,_ = req_get("fixtures", {"date": ds}, what=f"fixtures?date={ds}")
        for x in fx:
            lgid = int(x.get("league",{}).get("id",-1))
            if lgid not in lids: 
                continue
            if status_long(x) not in KEEP_STATUS:
                continue
            fid = int(x.get("fixture",{}).get("id"))
            season=int(x.get("league",{}).get("season") or 0)
            home_id = int(x.get("teams",{}).get("home",{}).get("id") or 0)
            away_id = int(x.get("teams",{}).get("away",{}).get("id") or 0)
            htH, htA = goals_ht(x)
            ftH, ftA = goals_ft(x)
            winner = winner_code(x)
            xgH, xgA = (None, None)
            if not args.no_xg:
                xgH, xgA = fetch_xg_or_estimate(fid)


            rec = {
                "date": ds,
                "season": season,
                "league_id": lgid,
                "fixture_id": fid,
                "kickoff_ts": x.get("fixture", {}).get("timestamp"),  # ← เพิ่มบรรทัดนี้
                "status": {
                    "short": x.get("fixture",{}).get("status",{}).get("short"),
                    "long" : x.get("fixture",{}).get("status",{}).get("long"),
                },
                "teams": {
                    "home": {"id": home_id, "name": x.get("teams",{}).get("home",{}).get("name")},
                    "away": {"id": away_id, "name": x.get("teams",{}).get("away",{}).get("name")},
                },
                "score": {
                    "ht": {"home": htH, "away": htA},
                    "ft": {"home": ftH, "away": ftA},
                    "winner": winner,
                },
                "xg": {"home": xgH, "away": xgA},
            }
            all_recs.append(rec)
        day += timedelta(days=1)

    start_s = start.strftime("%Y%m%d")
    end_s   = end.strftime("%Y%m%d")
    jpath = outdir / f"results_full_{start_s}_{end_s}.json"
    with open(jpath,"w",encoding="utf-8") as w:
        json.dump({"fixtures": all_recs, "meta":{
            "date_from": start.strftime("%Y-%m-%d"),
            "date_to":   end.strftime("%Y-%m-%d"),
            "vendor": VENDOR,
            "leagues": len(lids),
            "xg": (not args.no_xg)
        }}, w, ensure_ascii=False)
    print(f"✅ JSON saved: {jpath} | fixtures={len(all_recs)}")

if __name__=="__main__":
    main()
