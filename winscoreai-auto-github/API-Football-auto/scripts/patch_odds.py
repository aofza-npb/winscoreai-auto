# scripts/patch_odds.py
# -*- coding: utf-8 -*-
"""
Patch odds JSON into Firebase with per-fixture summaries & features.

Input:
  --json PATH    : odds_full_YYYYMMDD_YYYYMMDD.json (จาก af_today_odds.py)
  --bm INT       : (optional) id เจ้ามือที่อยากใช้เป็นแหล่งหลักของฟีเจอร์ (เช่น 6 = Bet365)
  --dry-run      : แสดง preview ไม่เขียน Firebase
  --monitor-path : path สำหรับเก็บสรุปการรันใน Firebase (default: monitoring/odds/last_run)

เขียนไปที่:
  matches/{league_id}/{fixture_id}/odds               -> เก็บ RAW bookmakers ทั้งก้อน + meta ingest
  matches/{league_id}/{fixture_id}/odds_features      -> ฟีเจอร์/สรุปต่อแมตช์
  {monitor-path}                                      -> สรุปการรัน

Env (เวลาเขียนจริง):
  FIREBASE_CREDENTIALS  : base64 ของ service account JSON
  FIREBASE_DATABASE_URL : URL ของ RTDB/Firestore REST (แล้วแต่ fb_client ของคุณ)
"""

import os
import json
import math
import argparse
from statistics import mean, pstdev
from datetime import datetime, timezone

# ---------- FB client (optional) ----------
def load_fb_update_multi():
    try:
        from fb_client import update_multi  # ฟังก์ชันของโปรเจกต์คุณเอง
        return update_multi
    except Exception as e:
        print("⚠️ fb_client.update_multi not available:", e)
        return None

# ---------- small utils ----------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def safe_float(x):
    try:
        return float(str(x))
    except Exception:
        return None

def implied_prob(odd):
    o = safe_float(odd)
    if not o or o <= 1e-9:
        return None
    return 1.0 / o

def overround(pH, pD, pA):
    comps = [p for p in (pH, pD, pA) if p is not None]
    return sum(comps) if comps else None

def entropy_probs(ps):
    vals = [p for p in ps if p is not None and p > 0]
    s = sum(vals)
    if s <= 0:
        return None
    return -sum((p/s) * math.log(p/s) for p in vals)

def pick_best_ou_line(ou_map):
    """เลือก line ที่นิยม: 2.5 > 2.75 > 3.0 > 2.25 > 3.25; ไม่มีก็เลือกใกล้ 2.5"""
    if not ou_map:
        return None, None
    for pref in ("2.5", "2.75", "3", "2.25", "3.25"):
        if pref in ou_map:
            return pref, ou_map[pref]
    try:
        k = sorted(ou_map.keys(), key=lambda x: abs(float(x) - 2.5))[0]
    except Exception:
        k = list(ou_map.keys())[0]
    return k, ou_map[k]

def pick_best_hcp_line(hcp_map):
    """เลือก line ยอดนิยม: -0.25 / 0 / +0.25; ไม่มีก็เลือกใกล้ 0"""
    if not hcp_map:
        return None, None
    for pref in ("-0.25", "0", "0.25", "+0.25"):
        if pref in hcp_map:
            return pref, hcp_map[pref]
    try:
        k = sorted(hcp_map.keys(), key=lambda x: abs(float(x)))[0]
    except Exception:
        k = list(hcp_map.keys())[0]
    return k, hcp_map[k]

def bookmaker_stats_1x2(books):
    """รวมทุกเจ้ามือ → สถิติของ home/draw/away odds + implied เฉลี่ย/overround/entropy"""
    home_odds, draw_odds, away_odds = [], [], []
    pH_list, pD_list, pA_list = [], [], []
    for _, mk in books.items():
        one = mk.get("1x2") or {}
        h = safe_float(one.get("home"))
        d = safe_float(one.get("draw"))
        a = safe_float(one.get("away"))
        if h: home_odds.append(h)
        if d: draw_odds.append(d)
        if a: away_odds.append(a)
        ph, pd, pa = implied_prob(h), implied_prob(d), implied_prob(a)
        if ph is not None: pH_list.append(ph)
        if pd is not None: pD_list.append(pd)
        if pa is not None: pA_list.append(pa)

    def agg(vals):
        if not vals:
            return {"mean": None, "stdev": None, "min": None, "max": None, "count": 0, "spread": None}
        return {
            "mean": mean(vals),
            "stdev": (pstdev(vals) if len(vals) > 1 else 0.0),
            "min": min(vals),
            "max": max(vals),
            "count": len(vals),
            "spread": (max(vals) - min(vals)) if len(vals) >= 2 else 0.0,
        }

    pH = (mean(pH_list) if pH_list else None)
    pD = (mean(pD_list) if pD_list else None)
    pA = (mean(pA_list) if pA_list else None)
    ov = overround(pH, pD, pA)
    ent = entropy_probs([pH, pD, pA])

    return {
        "odds": {
            "home": agg(home_odds),
            "draw": agg(draw_odds),
            "away": agg(away_odds),
        },
        "implied_avg": {"home": pH, "draw": pD, "away": pA, "overround": ov, "entropy": ent},
    }

def completeness_flags(has_1x2, has_ou, has_hcp):
    return {
        "has_1x2": bool(has_1x2),
        "has_ou": bool(has_ou),
        "has_hcp": bool(has_hcp),
        "complete_3_markets": bool(has_1x2 and has_ou and has_hcp),
    }

# ---------- core: make features for ONE fixture ----------
def build_features_per_fixture(rec, bm_prefer=None, keep_bm: int = 5):
    lid = rec["league_id"]; fid = rec["fixture_id"]
    books_all = rec.get("bookmakers", {}) or {}
    books = select_bookmakers(books_all, keep=keep_bm)  # <<< กรองเหลือ 5 เจ้า

    # เลือกเจ้ามือสำหรับฟีเจอร์ (ถ้าระบุ --bm ให้ใช้เลย, ไม่งั้นเลือกที่มี 1x2/OU/HCP มากสุด)
    chosen_bm = None
    if bm_prefer and str(bm_prefer) in books:
        chosen_bm = str(bm_prefer)
    else:
        best_score = -1
        for bmid, mk in books.items():
            score = int(bool(mk.get("1x2"))) + int(bool(mk.get("ou"))) + int(bool(mk.get("hcp")))
            if score > best_score:
                best_score = score
                chosen_bm = bmid

    mk = books.get(chosen_bm or "", {}) if books else {}
    one_line = mk.get("1x2") or {}
    ou_line_key, ou_sel = pick_best_ou_line(mk.get("ou") or {})
    hcp_line_key, hcp_sel = pick_best_hcp_line(mk.get("hcp") or {})

    # implied จาก 1x2 ของ “chosen”
    def _i(x): 
        p = implied_prob(x)
        return p if p is not None else None
    h = _i(one_line.get("home"))
    d = _i(one_line.get("draw"))
    a = _i(one_line.get("away"))
    tot = sum([p for p in (h, d, a) if p is not None]) or None
    implied = {
        "home": (h / tot) if (h is not None and tot) else None,
        "draw": (d / tot) if (d is not None and tot) else None,
        "away": (a / tot) if (a is not None and tot) else None,
        "overround": tot,
    }

    markets_present = sorted({k for _, m in books.items() for k in (m.keys() if m else [])})
    xbm = bookmaker_stats_1x2(books)

    features = {
        "source_bookmaker_id": (int(chosen_bm) if chosen_bm is not None else None),
        "markets_present": markets_present,
        "n_bookmakers": len(books),
        "n_markets": len(markets_present),
        "one": one_line,  # ของ chosen bookmaker
        "ou": {"line": ou_line_key, **(ou_sel or {})} if ou_line_key else {"line": None},
        "hcp": {"line": hcp_line_key, **(hcp_sel or {})} if hcp_line_key else {"line": None},
        "implied": implied,
        "xbm_1x2": xbm,
        "flags": completeness_flags(bool(one_line), bool(ou_line_key), bool(hcp_line_key)),
        "meta": {"updated_at": now_iso()},

    }
    # เผื่อเอาไว้ดูคีย์หลัก (ไม่กระทบ Firebase)
    features["_ids"] = {"league_id": lid, "fixture_id": fid}
    features["kickoff_ts"] = rec.get("kickoff_ts")

    return features
# เลือก top-N เจ้ามือ ตาม priority และความครบตลาด
PRIORITY = (6, 1, 7, 8, 2)  # ตัวอย่าง: Bet365(6) > Pinnacle(1) > 1xBet(7) > 888(8) > 10bet(2)

def select_bookmakers(books: dict, keep: int = 5, priority: tuple = PRIORITY) -> dict:
    if keep <= 0 or not books:
        return books
    # ให้คะแนน: ติดอันดับใน priority มาก่อน, แล้วค่อยจำนวนตลาดที่มี (1x2/ou/hcp)
    def score(item):
        bmid, mk = item
        try:
            pr = priority.index(int(bmid))
        except Exception:
            pr = len(priority)  # ไม่มีใน priority = ลำดับท้าย
        markets_cnt = sum(1 for k in ("1x2","ou","hcp") if mk.get(k))
        return (pr, -markets_cnt)  # pr น้อยดีกว่า, markets มากดีกว่า
    top = sorted(books.items(), key=score)[:keep]
    return {k: v for k, v in top}

# ---------- preview helper ----------
def preview_updates(updates, limit=8):
    print("\n---- PREVIEW (first {} nodes) ----".format(limit))
    keys = list(updates.keys())[:limit]
    for k in keys:
        v = updates[k]
        s = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
        print(k, "=>", (s[:240] + ("..." if len(s) > 240 else "")))
    print("---- END PREVIEW ----")
    return True

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to odds_full_*.json (from af_today_odds.py)")
    ap.add_argument("--bm", type=int, default=0, help="preferred bookmaker id (0 = auto)")
    ap.add_argument("--dry-run", action="store_true", help="preview only")
    ap.add_argument("--monitor-path", default="monitoring/odds/last_run", help="Firebase path for run summary")
    args = ap.parse_args()

    with open(args.json, encoding="utf-8") as f:
        payload = json.load(f)

    fixtures = payload.get("fixtures", []) or []
    meta_in = payload.get("meta", {}) or {}
    print(f"อ่านไฟล์: {args.json} | fixtures={len(fixtures)}")

    updates = {}
    n_odds_nodes = 0
    n_feat_nodes = 0

    for rec in fixtures:
        lid = int(rec["league_id"])
        fid = int(rec["fixture_id"])

        
        n_odds_nodes += 1

        # 2) คำนวณ features ต่อแมตช์
        feat = build_features_per_fixture(rec, bm_prefer=(args.bm or None))
        updates[f"matches/{lid}/{fid}/odds_features"] = feat
        updates[f"matches/{lid}/{fid}/kickoff_ts"] = rec.get("kickoff_ts")
        n_feat_nodes += 1

    # 3) monitoring
    summary = {
        "run_at": now_iso(),
        "input_json": os.path.basename(args.json),
        "fixtures": len(fixtures),
        "odds_nodes": n_odds_nodes,
        "feature_nodes": n_feat_nodes,
        "bookmaker_preferred": (args.bm or None),
        "meta_in": meta_in,
        "env": {
            "GITHUB_WORKFLOW": os.getenv("GITHUB_WORKFLOW"),
            "GITHUB_JOB": os.getenv("GITHUB_JOB"),
            "GITHUB_SHA": os.getenv("GITHUB_SHA"),
            "GITHUB_REF": os.getenv("GITHUB_REF"),
            "GITHUB_REPOSITORY": os.getenv("GITHUB_REPOSITORY"),
        }
    }
    updates[args.monitor_path] = summary

    # preview
    preview_updates(updates)

    if args.dry_run:
        print("\nDRY-RUN: ข้ามการเขียน Firebase")
        return

    update_multi = load_fb_update_multi()
    if not update_multi:
        print("⚠️ ไม่พบ fb_client.update_multi — ข้ามการเขียน Firebase (พิมพ์อย่างเดียว)")
        return

    ok = update_multi(updates)
    print("\n✅ Firebase update_multi:", ok)
    print("สรุป:", json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
