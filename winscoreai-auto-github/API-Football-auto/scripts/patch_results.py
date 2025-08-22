# scripts/patch_results.py
# -*- coding: utf-8 -*-
"""
Patch finished results (HT/FT + xG) into Firebase:
 - matches/{league_id}/{fixture_id}/result  (single source of truth)
 - (optional) matches/{league_id}/{fixture_id}/results  (legacy mirror)
 - teams/{team_id}/form/last5/{fixture_id}
 - teams/{team_id}/summary                        (aggregate last N, default 5)
 - (optional) simple indexes under /idx/*

Usage:
  python .\scripts\patch_results.py --json results\results_full_YYYYMMDD_YYYYMMDD.json
    [--mirror-old] [--index] [--last N] [--dry-run]

Env needed when writing:
  FIREBASE_CREDENTIALS  (base64 of service-account JSON)  OR  FIREBASE_SA_PATH
  FIREBASE_DATABASE_URL
"""

import os, sys, json, argparse
from datetime import datetime, timezone
from collections import defaultdict

ISO = lambda: datetime.now(timezone.utc).isoformat()

# -------- fb client (optional) ----------
def load_update_multi():
    try:
        from fb_client import update_multi  # your existing robust writer
        return update_multi
    except Exception as e:
        print("⚠️ fb_client.update_multi not available:", e)
        return None

def preview(updates, limit=12):
    keys = list(updates.keys())
    print("---- PREVIEW (first %d nodes) ----" % min(limit, len(keys)))
    for k in keys[:limit]:
        v = updates[k]
        sv = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
        print(f"{k} => {sv[:220]}{'...' if len(sv)>220 else ''}")
    print("---- END PREVIEW ----")

# -------- helpers ----------
def as_float(x):
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x))
        except Exception:
            return None

def wdl_and_pts(ft_h, ft_a):
    if ft_h is None or ft_a is None:
        return "D", 0  # safe fallback
    if ft_h > ft_a: return "W", 3
    if ft_h < ft_a: return "L", 0
    return "D", 1

def team_entry(team_obj):
    if not team_obj: return {"id": 0, "name": ""}
    return {
        "id": int(team_obj.get("id") or 0),
        "name": team_obj.get("name") or ""
    }

def parse_fixture(rec):
    """normalize one fixture from results_full JSON"""
    lid = int(rec.get("league_id"))
    fid = int(rec.get("fixture_id"))
    date = rec.get("date")
    season = int(rec.get("season") or 0)

    teams = rec.get("teams", {}) or {}
    t_home = team_entry(teams.get("home"))
    t_away = team_entry(teams.get("away"))

    ht = rec.get("score", {}).get("ht") or rec.get("score", {}).get("halftime") or {}
    ft = rec.get("score", {}).get("ft") or rec.get("goals") or {}
    ht_h, ht_a = ht.get("home"), ht.get("away")
    ft_h, ft_a = ft.get("home"), ft.get("away")

    winner = rec.get("score", {}).get("winner") or rec.get("winner") or ""
    if not winner:
        if ft_h is not None and ft_a is not None:
            if ft_h > ft_a: winner = "home"
            elif ft_h < ft_a: winner = "away"
            else: winner = "draw"
        else:
            winner = "draw"

    xg = rec.get("xg", {}) or {}
    xgh = as_float(xg.get("home"))
    xga = as_float(xg.get("away"))

    return {
        "league_id": lid,
        "fixture_id": fid,
        "date": date,
        "season": season,
        "teams": {"home": t_home, "away": t_away},
        "ht": {"h": ht_h, "a": ht_a},
        "ft": {"h": ft_h, "a": ft_a},
        "winner": winner,
        "xg": {"h": xgh, "a": xga},
        "kickoff_ts": rec.get("kickoff_ts"),  # ← รับค่าที่ af_results.py ใส่มา
    }

# -------- team form builder (from batch fixtures) --------
def build_team_forms(fixtures, last_n=5):
    """
    fixtures: list of normalized fixtures (from parse_fixture)
    return: dict team_id -> {
        "last5": {fixture_id: {...}},
        "summary": {...}
    }
    """
    # collect per team, per date
    by_team = defaultdict(list)
    for r in fixtures:
        fid = r["fixture_id"]
        date = r["date"]
        lid = r["league_id"]
        h = r["teams"]["home"]["id"]
        a = r["teams"]["away"]["id"]
        ht = r["ht"]; ft = r["ft"]
        xg = r["xg"]

        # home perspective
        resH, ptsH = wdl_and_pts(ft["h"], ft["a"])
        by_team[h].append({
            "date": date, "fixture_id": fid, "league_id": lid,
            "opp_id": a,
            "res": resH, "pts": ptsH,
            "gf": ft["h"] or 0, "ga": ft["a"] or 0, "gd": (0 if ft["h"] is None or ft["a"] is None else (ft["h"]-ft["a"])),
            "xg_for": xg["h"] if xg["h"] is not None else 0.0,
            "xg_against": xg["a"] if xg["a"] is not None else 0.0,
            "xg_diff": ( (xg["h"] or 0.0) - (xg["a"] or 0.0) ),
        })
        # away perspective
        resA, ptsA = wdl_and_pts(ft["a"], ft["h"])
        by_team[a].append({
            "date": date, "fixture_id": fid, "league_id": lid,
            "opp_id": h,
            "res": resA, "pts": ptsA,
            "gf": ft["a"] or 0, "ga": ft["h"] or 0, "gd": (0 if ft["h"] is None or ft["a"] is None else (ft["a"]-ft["h"])),
            "xg_for": xg["a"] if xg["a"] is not None else 0.0,
            "xg_against": xg["h"] if xg["h"] is not None else 0.0,
            "xg_diff": ( (xg["a"] or 0.0) - (xg["h"] or 0.0) ),
        })

    # sort by date and take last N, then aggregate
    out = {}
    for tid, rows in by_team.items():
        rows.sort(key=lambda r: r["date"])  # asc
        last = rows[-last_n:] if last_n > 0 else rows[:]
        # build last5 map
        last_map = {int(r["fixture_id"]): {
            "date": r["date"], "league_id": r["league_id"], "opp_id": r["opp_id"],
            "res": r["res"], "pts": r["pts"],
            "gf": r["gf"], "ga": r["ga"], "gd": r["gd"],
            "xg_for": round(float(r["xg_for"]), 3),
            "xg_against": round(float(r["xg_against"]), 3),
            "xg_diff": round(float(r["xg_diff"]), 3),
        } for r in last}

        # aggregate summary
        W = sum(1 for r in last if r["res"] == "W")
        D = sum(1 for r in last if r["res"] == "D")
        L = sum(1 for r in last if r["res"] == "L")
        GF = sum(int(r["gf"]) for r in last)
        GA = sum(int(r["ga"]) for r in last)
        GD = GF - GA
        xG_for = round(sum(float(r["xg_for"]) for r in last), 3)
        xG_against = round(sum(float(r["xg_against"]) for r in last), 3)
        xG_diff = round(xG_for - xG_against, 3)

        out[int(tid)] = {
            "last5": last_map,
            "summary": {
                "n": len(last),
                "W": W, "D": D, "L": L,
                "GF": GF, "GA": GA, "GD": GD,
                "xG_for": xG_for, "xG_against": xG_against, "xG_diff": xG_diff,
                "updated_at": ISO(),
            }
        }
    return out

# -------- main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="results_full_*.json from af_results.py")
    ap.add_argument("--mirror-old", action="store_true", help="also write legacy matches/*/results")
    ap.add_argument("--index", action="store_true", help="write simple indexes under /idx/*")
    ap.add_argument("--last", type=int, default=5, help="team form windows (default=5)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(args.json, encoding="utf-8") as f:
        payload = json.load(f)

    fixtures_in = payload.get("fixtures", [])
    print(f"อ่าน: {args.json} | fixtures={len(fixtures_in)}")

    # normalize all fixtures
    fixtures = [parse_fixture(rec) for rec in fixtures_in]

    updates = {}

    # 1) matches/{lid}/{fid}/result  (+ optional legacy mirror)
    for r in fixtures:
        lid = r["league_id"]; fid = r["fixture_id"]
        node = f"matches/{lid}/{fid}/result"
        updates[node] = {
            "date": r["date"],
            "kickoff_ts": r.get("kickoff_ts"),  # ← เพิ่มบรรทัดนี้
            "season": r["season"],
            "league_id": lid,
            "fixture_id": fid,
            "teams": r["teams"],
            "ht": r["ht"],
            "ft": r["ft"],
            "winner": r["winner"],
            "xg": {"home": r["xg"]["h"], "away": r["xg"]["a"]},
            "meta": {
                "ingested_at": ISO(),
                "source": "api-sports-v3",
            }
        }
        if args.mirror_old:
            legacy = f"matches/{lid}/{fid}/results"
            updates[legacy] = {
                "date": r["date"],
                "season": r["season"],
                "teams": {
                    "home": r["teams"]["home"]["name"],
                    "away": r["teams"]["away"]["name"],
                },
                "score": {
                    "ht": {"home": r["ht"]["h"], "away": r["ht"]["a"]},
                    "ft": {"home": r["ft"]["h"], "away": r["ft"]["a"]},
                    "winner": r["winner"],
                },
                "xg": {"home": r["xg"]["h"], "away": r["xg"]["a"]},
                "ingested_at": ISO()
            }
            

            

    # 2) team forms (last N) + summary (single path)
    team_forms = build_team_forms(fixtures, last_n=args.last)
    for tid, obj in team_forms.items():
        # last5
        for fid, row in obj["last5"].items():
            updates[f"teams/{tid}/form/last5/{fid}"] = row
        # single summary path (แก้เรื่องมี 2 summary → เขียน path เดียว)
        updates[f"teams/{tid}/summary"] = obj["summary"]

    # 3) (optional) indexes for faster lookup
    if args.index:
        for r in fixtures:
            ds = r["date"]; lid = r["league_id"]; fid = r["fixture_id"]
            h = r["teams"]["home"]["id"]; a = r["teams"]["away"]["id"]
            updates[f"idx/date_fixtures/{ds}/{fid}"] = True
            updates[f"idx/team_fixtures/{h}/{ds}/{fid}"] = True
            updates[f"idx/team_fixtures/{a}/{ds}/{fid}"] = True
            updates[f"idx/league_fixtures/{lid}/{ds}/{fid}"] = True

    # summary/monitor
    updates["monitoring/results/last_run"] = {
        "run_at": ISO(),
        "input_json": os.path.basename(args.json),
        "fixtures": len(fixtures),
        "last_window": args.last,
        "mirror_old": bool(args.mirror_old),
        "indexed": bool(args.index),
        "env": {
            "GITHUB_WORKFLOW": os.getenv("GITHUB_WORKFLOW"),
            "GITHUB_JOB": os.getenv("GITHUB_JOB"),
            "GITHUB_SHA": os.getenv("GITHUB_SHA"),
            "GITHUB_REF": os.getenv("GITHUB_REF"),
            "GITHUB_REPOSITORY": os.getenv("GITHUB_REPOSITORY"),
        }
    }

    # show
    preview(updates)

    if args.dry_run:
        print("DRY-RUN: ข้ามการเขียน Firebase")
        return

    update_multi = load_update_multi()
    if not update_multi:
        print("⚠️ ไม่พบ fb_client.update_multi — ข้ามการเขียน Firebase (พิมพ์อย่างเดียว)")
        return

    ok = update_multi(updates)
    print("\n✅ Firebase update_multi:", ok)
    print("keys_total:", len(updates))

if __name__ == "__main__":
    main()
