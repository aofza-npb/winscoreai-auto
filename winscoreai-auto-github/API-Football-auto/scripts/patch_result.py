# API-Football/scripts/patch_result.py
import os, sys, json, argparse, time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

import requests
from firebase_admin import db
from fb_client import update_multi

API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

FINISHED_STATES = {"Match Finished", "AET", "Penalty", "Awarded", "WO", "Abandoned"}

# -------------------- Utilities --------------------
def get(url, params, what="", max_retry=4, wait=1.2):
    for i in range(max_retry):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            j = r.json()
            return j.get("response", []), j.get("errors", {})
        time.sleep(wait * (1 + 0.5*i))
    raise RuntimeError(f"GET {what or url} failed after retries")

def safe(x):
    return None if x in ("", None) else x

def read_node(path: str) -> dict:
    return db.reference(path).get() or {}

def winner_from_fixture_like(goals: dict, teams: dict) -> str:
    gh = goals.get("home"); ga = goals.get("away")
    if isinstance(gh, (int, float)) and isinstance(ga, (int, float)):
        if gh > ga: return "Home"
        if ga > gh: return "Away"
        return "Draw"
    # fallback
    w_home = teams.get("home", {}).get("winner")
    w_away = teams.get("away", {}).get("winner")
    if w_home is True: return "Home"
    if w_away is True: return "Away"
    return ""

# -------------------- xG Sources --------------------
def compute_xg_from_understat(us_node: dict) -> Optional[Dict[str, float]]:
    if not us_node: return None
    h = us_node.get("xG_home"); a = us_node.get("xG_away")
    if isinstance(h, (int,float)) and isinstance(a, (int,float)):
        return {"home": float(h), "away": float(a)}
    shots = us_node.get("shots") or []
    if isinstance(shots, list) and shots:
        hs = sum(float(s.get("xG",0) or 0) for s in shots if str(s.get("team","")).lower() in ("home","h","host"))
        as_ = sum(float(s.get("xG",0) or 0) for s in shots if str(s.get("team","")).lower() in ("away","a","guest"))
        return {"home": round(hs,3), "away": round(as_,3)}
    return None

def compute_xg_from_footystats(fs_node: dict) -> Optional[Dict[str, float]]:
    if not fs_node: return None
    for kh, ka in [
        ("Home Team Pre-Match xG", "Away Team Pre-Match xG"),
        ("home_team_pre_match_xg", "away_team_pre_match_xg"),
        ("pre_match_xg_home", "pre_match_xg_away"),
    ]:
        try:
            h = fs_node.get(kh); a = fs_node.get(ka)
            if h not in (None,"") and a not in (None,""):
                return {"home": round(float(h),3), "away": round(float(a),3)}
        except: pass
    return None

def _num(x):
    try:
        if isinstance(x, str) and x.endswith("%"): return float(x.strip("%"))
        return float(x)
    except: return 0.0

def compute_xg_heuristic_from_stats(stats_home: dict, stats_away: dict, score: dict) -> Optional[Dict[str, float]]:
    """Improved heuristic xG using team stats (see recipe in the message)."""
    if not (stats_home or stats_away): return None

    def team_xg(me: dict, opp: dict) -> float:
        shots_total     = _num(me.get("shots_total"))
        shots_on_goal   = _num(me.get("shots_on_goal"))
        blocked_shots   = _num(me.get("blocked_shots"))
        shots_off_goal  = _num(me.get("shots_off_goal"))
        shots_insidebox = _num(me.get("shots_insidebox"))
        big_chances     = _num(me.get("big_chances"))
        pens            = _num(me.get("penalties"))
        poss            = _num(me.get("possession_pct"))
        red_me          = _num(me.get("red_cards"))
        red_opp         = _num(opp.get("red_cards"))

        # derive if missing
        if shots_off_goal == 0 and shots_total > 0:
            shots_off_goal = max(shots_total - shots_on_goal - blocked_shots, 0)

        inside_bonus = max(shots_insidebox - shots_on_goal, 0)
        pen_xg = 0.76 * pens

        base = 0.13*shots_on_goal + 0.06*blocked_shots + 0.03*shots_off_goal \
               + 0.18*big_chances + 0.05*inside_bonus + pen_xg

        pos_mult = 1.0 + 0.003 * (poss - 50.0)  # +/- 0.3 per +100% (เล็กน้อย)
        rc_adv = max(0.0, red_opp - red_me)     # ได้เปรียบใบแดง
        rc_mult = 1.0 + 0.05 * rc_adv

        return max(0.0, round(base * pos_mult * rc_mult, 3))

    xg_h = team_xg(stats_home or {}, stats_away or {})
    xg_a = team_xg(stats_away or {}, stats_home or {})

    return {"home": xg_h, "away": xg_a}

def compute_xg_heuristic_fallback(en_node: dict) -> Optional[Dict[str, float]]:
    """เก็บ fallback เก่าจาก enriched ถ้าไม่มี stats"""
    if not en_node: return None
    try:
        h_sh  = float(en_node.get("home_team_shots", 0) or 0)
        h_sot = float(en_node.get("home_team_shots_on_target", 0) or 0)
        a_sh  = float(en_node.get("away_team_shots", 0) or 0)
        a_sot = float(en_node.get("away_team_shots_on_target", 0) or 0)
    except: return None
    def est(sh, sot):
        off = max(sh - sot, 0)
        return round(0.10 * sot + 0.04 * off, 3)
    return {"home": est(h_sh, h_sot), "away": est(a_sh, a_sot)}

# -------------------- Patch logic --------------------
def build_result_payload_from_json_item(item: dict) -> Dict[str, Any]:
    return {
        "goals": item.get("goals") or {},
        "score": item.get("score") or {},
        "status": item.get("status_long"),
        "winner": item.get("winner"),
        # เก็บ stats ไว้ใต้ result ด้วย (ใช้ภายหลัง/ตรวจสอบย้อนกลับ)
        "stats": item.get("stats") or {},
    }

def patch_one_fixture(league_id: int, fixture_id: int, res_payload: dict, do_xg=True, dry_run=False):
    base = f"matches/{league_id}/{fixture_id}/result"
    ts_iso = datetime.now(timezone.utc).isoformat()

    updates: Dict[str, Any] = {}
    for k, v in res_payload.items():
        updates[f"{base}/{k}"] = v
    updates[f"{base}/meta/updated_at"] = ts_iso
    updates[f"{base}/meta/source"] = "api-sports-v3"

    if do_xg:
        # 1) Understat → postmatch
        us = read_node(f"matches/{league_id}/{fixture_id}/understat")
        xg_us = compute_xg_from_understat(us)
        if xg_us:
            updates[f"{base}/xg/postmatch/home"] = xg_us["home"]
            updates[f"{base}/xg/postmatch/away"] = xg_us["away"]
            updates[f"{base}/xg/method"] = "understat"
        else:
            # 2) FootyStats → prematch
            fs = read_node(f"matches/{league_id}/{fixture_id}/footystats")
            xg_fs = compute_xg_from_footystats(fs)
            if xg_fs:
                updates[f"{base}/xg/prematch/home"] = xg_fs["home"]
                updates[f"{base}/xg/prematch/away"] = xg_fs["away"]
                updates[f"{base}/xg/method"] = "footystats"
            else:
                # 3) Heuristic (stats-first → enriched-fallback)
                stats = res_payload.get("stats") or read_node(f"{base}/stats")
                if stats:
                    xg_h = compute_xg_heuristic_from_stats(stats.get("home", {}), stats.get("away", {}), res_payload.get("score") or {})
                else:
                    xg_h = None
                if not xg_h:
                    en = read_node(f"matches/{league_id}/{fixture_id}/api_football_enriched")
                    xg_h = compute_xg_heuristic_fallback(en)
                if xg_h:
                    updates[f"{base}/xg/postmatch/home"] = xg_h["home"]
                    updates[f"{base}/xg/postmatch/away"] = xg_h["away"]
                    # ถ้ามี prematch จาก FS อยู่แล้วจะเก็บทับตามแหล่ง
                    if "xg/method" not in updates:
                        updates[f"{base}/xg/method"] = "heuristic"

    if dry_run:
        print(f"[DRY] {league_id}/{fixture_id} KEYS={len(updates)}")
        for i, (k, v) in enumerate(updates.items()):
            if i >= 14: break
            print(" ", k, "=>", v)
        return

    #db.reference("/").update(updates)
    update_multi(updates)
    print(f"✅ patched → matches/{league_id}/{fixture_id}/result")

# -------------------- Entrypoints --------------------
def run_from_json(json_path: str, do_xg=True, dry_run=False):
    with open(json_path, "r", encoding="utf-8") as f:
        items = json.load(f)
    for it in items:
        lid = int(it["league_id"]); fid = int(it["fixture_id"])
        payload = build_result_payload_from_json_item(it)
        patch_one_fixture(lid, fid, payload, do_xg=do_xg, dry_run=dry_run)

def run_single_fixture(fixture_id: int, league_id: int, do_xg=True, dry_run=False):
    resp, _ = get(f"{BASE}/fixtures", {"id": fixture_id}, what=f"fixture {fixture_id}")
    if not resp:
        print("ไม่พบ fixture จาก API"); return
    fx = resp[0]
    goals = fx.get("goals") or {}
    payload = {
        "goals": {"home": goals.get("home"), "away": goals.get("away")},
        "score": fx.get("score") or {},
        "status": (fx.get("fixture", {}).get("status", {}) or {}).get("long"),
        "winner": winner_from_fixture_like(goals, fx.get("teams") or {}),
        # *ไม่มี stats จาก endpoint นี้ใน call เดียว — ถ้าต้องการให้ครบ แนะนำใช้ JSON โหมดจาก af_results.py
        "stats": {},
    }
    patch_one_fixture(league_id, fixture_id, payload, do_xg=do_xg, dry_run=dry_run)

def run_by_dates(start_date: str, days: int, do_xg=True, dry_run=False):
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    for i in range(days):
        d = start + timedelta(days=i)
        ds = d.strftime("%Y-%m-%d")
        fx, _ = get(f"{BASE}/fixtures", {"date": ds}, what=f"fixtures {ds}")
        finished = [x for x in fx if (x.get("fixture", {}).get("status", {}).get("long") in FINISHED_STATES)]
        print(f"• {ds} finished: {len(finished)}")
        for f in finished:
            lid = int(f.get("league", {}).get("id"))
            fid = int(f.get("fixture", {}).get("id"))
            payload = {
                "goals": f.get("goals") or {},
                "score": f.get("score") or {},
                "status": (f.get("fixture", {}).get("status", {}) or {}).get("long"),
                "winner": winner_from_fixture_like(f.get("goals") or {}, f.get("teams") or {}),
                "stats": {},  # ถ้าต้องการ stats ควรใช้ af_results.py สร้าง JSON แล้วโหมด --json
            }
            patch_one_fixture(lid, fid, payload, do_xg=do_xg, dry_run=dry_run)
        time.sleep(0.2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="อ่านผลจาก JSON ของ af_results.py (มี stats พร้อม) แนะนำ!")
    ap.add_argument("--fixture", type=int, help="fixture id เดี่ยว (ใช้คู่กับ --league)")
    ap.add_argument("--league",  type=int, help="league id (ใช้คู่กับ --fixture)")
    ap.add_argument("--date", help="YYYY-MM-DD ใช้โหมดดึงตามวัน (ยิง API)")
    ap.add_argument("--days", type=int, default=1, help="จำนวนวัน (เฉพาะโหมด --date)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-xg", action="store_true", help="ไม่คำนวณ xG")
    args = ap.parse_args()

    init_firebase()

    if args.json:
        run_from_json(args.json, do_xg=not args.no_xg, dry_run=args.dry_run)
        return
    if args.fixture and args.league:
        if not API_KEY: raise RuntimeError("Missing API_FOOTBALL_KEY")
        run_single_fixture(args.fixture, args.league, do_xg=not args.no_xg, dry_run=args.dry_run)
        return
    if args.date:
        if not API_KEY: raise RuntimeError("Missing API_FOOTBALL_KEY")
        run_by_dates(args.date, args.days, do_xg=not args.no_xg, dry_run=args.dry_run)
        return

    print("โปรดระบุ --json หรือ --fixture/--league หรือ --date")
    sys.exit(1)

if __name__ == "__main__":
    main()
