# API-Football/scripts/af_results.py
# ดึงผลแข่ง "จบแล้ว" + สถิติรายทีม → บันทึก JSON/CSV เพื่อใช้กับ patch_result.py (xG heuristic)
import os, csv, json, time, argparse, re
from datetime import datetime, timedelta
from pathlib import Path
import requests

API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

FINISHED_STATES = {
    "Match Finished", "AET", "Penalty", "Awarded", "WO", "Abandoned"
}

def require_env():
    if not API_KEY:
        raise RuntimeError("ยังไม่พบ API_FOOTBALL_KEY")

def get(url, params, what="", max_retry=4, wait=1.2):
    for i in range(max_retry):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            j = r.json()
            return j.get("response", []), j.get("errors", {})
        time.sleep(wait * (1 + 0.5*i))
    raise RuntimeError(f"GET {what or url} failed after retries")

def read_allowlist(path: str) -> set[int]:
    lids = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            p = s.split()
            if p[0].isdigit(): lids.append(int(p[0]))
    return set(lids)

def status_long(rec):  # เหมือนเดิม
    return rec.get("fixture", {}).get("status", {}).get("long")

def stat_val(stats_list, key):
    """ดึงค่าจาก list [{'type': 'Total Shots','value': 10}, ...] โดย normalize ชื่อคีย์"""
    if not stats_list: return None
    for it in stats_list:
        t = (it.get("type") or "").strip().lower()
        v = it.get("value")
        if v in (None, ""): continue
        # normalize พื้นฐาน
        if t == key: return v
    return None

def normalize_stats(entry_list):
    """
    แปลงรูปแบบ response ของ /fixtures/statistics (สองฝั่ง) ให้อยู่ใน dict:
      { "home": {...}, "away": {...} }
    แล้ว normalize คีย์หลักให้พร้อมใช้
    """
    out = {"home": {}, "away": {}}
    if not entry_list: return out

    # API ปกติจะให้ 2 แถว [home, away]
    role_map = {}
    for ent in entry_list:
        team_side = "home" if ent.get("team", {}).get("name", "").lower() in ("home",) else None
        # บางครั้งไม่มีคำว่า home/away ใน team.name → ใช้ index แทน
    # ใช้วิธี map ตามลำดับแทนเพื่อความชัวร์
    sides = ["home", "away"]
    for i, ent in enumerate(entry_list[:2]):
        side = sides[i]
        slist = ent.get("statistics") or []
        def g(key_variants):
            for kv in key_variants:
                v = next((x.get("value") for x in slist if (x.get("type") or "").strip().lower()==kv), None)
                if v not in (None, ""): return v
            return None

        # สร้าง mapping เป็น lower-case key สำหรับค้นง่าย
        # รองรับคีย์ยอดฮิตของ API-Football
        def lc(s): return (s or "").strip().lower()
        kv = {lc(x.get("type")): x.get("value") for x in slist if x.get("type")}

        def get_num(*cands):
            for c in cands:
                v = kv.get(c)
                if v in (None, ""): continue
                if isinstance(v, str) and v.endswith("%"):
                    try:
                        return float(v.strip("%"))
                    except: continue
                try:
                    return float(v)
                except: pass
            return None

        out[side] = {
            "shots_total":     get_num("total shots", "shots total"),
            "shots_on_goal":   get_num("shots on goal", "shots on target"),
            "shots_off_goal":  get_num("shots off goal"),
            "blocked_shots":   get_num("blocked shots"),
            "shots_insidebox": get_num("shots insidebox", "shots inside box"),
            "shots_outsidebox":get_num("shots outsidebox", "shots outside box"),
            "big_chances":     get_num("big chances"),
            "penalties":       get_num("penalties", "penalty"),
            "possession_pct":  get_num("ball possession", "possession"),
            "red_cards":       get_num("red cards", "red card"),
        }

        # เติมค่าที่ขาดพื้นฐาน
        st = out[side]
        if st["shots_off_goal"] is None and st["shots_total"] is not None:
            sog = st["shots_on_goal"] or 0
            blk = st["blocked_shots"] or 0
            est = st["shots_total"] - sog - blk
            st["shots_off_goal"] = max(est, 0)

    return out

def winner_from_fixture(fx: dict) -> str:
    goals = fx.get("goals") or {}
    gh, ga = goals.get("home"), goals.get("away")
    if gh is None or ga is None:
        w_home = fx.get("teams", {}).get("home", {}).get("winner")
        w_away = fx.get("teams", {}).get("away", {}).get("winner")
        if w_home is True: return "Home"
        if w_away is True: return "Away"
        return ""
    if gh > ga: return "Home"
    if ga > gh: return "Away"
    return "Draw"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date_from", default=None, help="YYYY-MM-DD UTC")
    ap.add_argument("--date_to", default=None, help="YYYY-MM-DD UTC")
    ap.add_argument("--days", type=int, default=1, help="ถ้าไม่กำหนดจาก-ถึง ใช้วันนี้ย้อนหลัง days-1")
    ap.add_argument("--allow", default="allowlist_ALL.txt")
    ap.add_argument("--outdir", default="../data")
    ap.add_argument("--sleep", type=float, default=0.15)
    args = ap.parse_args()

    require_env()
    allow = read_allowlist(args.allow)

    # กำหนดช่วงวัน
    if args.date_from and args.date_to:
        start = datetime.strptime(args.date_from, "%Y-%m-%d").date()
        end   = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    else:
        end = datetime.utcnow().date()
        start = end - timedelta(days=args.days-1)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    tag = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    json_path = outdir / f"results_full_{tag}.json"
    csv_path  = outdir / f"results_full_{tag}.csv"

    results = []
    csv_rows = []

    day = start
    while day <= end:
        ds = day.strftime("%Y-%m-%d")
        fx, _ = get(f"{BASE}/fixtures", {"date": ds}, what=f"fixtures {ds}")
        fx = [x for x in fx
              if int(str(x.get('league', {}).get('id') or -1)) in allow
              and (x.get('fixture', {}).get('status', {}).get('long') in FINISHED_STATES)]
        print(f"• {ds} finished: {len(fx)}")

        for f in fx:
            fid = int(f.get("fixture", {}).get("id"))
            lid = int(f.get("league", {}).get("id"))
            season = int(f.get("league", {}).get("season", 0) or 0)
            home = f.get("teams", {}).get("home", {}).get("name", "")
            away = f.get("teams", {}).get("away", {}).get("name", "")
            st_long = f.get("fixture", {}).get("status", {}).get("long")
            goals = f.get("goals") or {}
            score = f.get("score") or {}

            # ดึงสถิติรายทีม
            stats_resp, _ = get(f"{BASE}/fixtures/statistics", {"fixture": fid}, what=f"stats {fid}")
            stats_norm = normalize_stats(stats_resp)

            node = {
                "season": season,
                "date": ds,
                "league_id": lid,
                "fixture_id": fid,
                "home": home,
                "away": away,
                "status_long": st_long,
                "goals": {"home": goals.get("home"), "away": goals.get("away")},
                "score": score,
                "winner": winner_from_fixture(f),
                "stats": stats_norm,   # <<< สำคัญสำหรับ heuristic xG
            }
            results.append(node)

            csv_rows.append([
                season, ds, lid, fid, home, away, st_long,
                goals.get("home", ""), goals.get("away", ""),
                (score.get("halftime") or {}).get("home", ""),
                (score.get("halftime") or {}).get("away", "")
            ])

            if args.sleep > 0:
                time.sleep(args.sleep)

        day += timedelta(days=1)

    with open(json_path, "w", encoding="utf-8") as wj:
        json.dump(results, wj, ensure_ascii=False, indent=2)
    with open(csv_path, "w", newline="", encoding="utf-8") as wc:
        wr = csv.writer(wc)
        wr.writerow(["season","date","league_id","fixture_id","home","away",
                     "status_long","goals_home","goals_away","ht_home","ht_away"])
        wr.writerows(csv_rows)

    print(f"✅ เขียน JSON: {json_path} (fixtures={len(results)})")
    print(f"✅ เขียน CSV : {csv_path} (rows={len(csv_rows)})")

if __name__ == "__main__":
    main()
