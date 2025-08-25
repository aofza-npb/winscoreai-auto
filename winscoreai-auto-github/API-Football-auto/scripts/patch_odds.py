# scripts/patch_odds.py
# -*- coding: utf-8 -*-
"""
Patch odds JSON (from af_today_odds.py) into Firebase with per-fixture summaries & features.

What it does:
- อ่านไฟล์ JSON: odds_full_YYYYMMDD_YYYYMMDD.json
- คำนวณฟีเจอร์ & สรุปต่อแมตช์:
    * ไม่ฟิกซ์ OU=2.5 หรือ HCP=-1 อีกต่อไป
    * เลือก "เส้นหลัก" แบบบาลานซ์ (over≈under สำหรับ OU, home≈away สำหรับ HCP)
    * เก็บทุกเส้นสำหรับ FT/HT: ou_all, hcp_all, ou_ht_all, hcp_ht_all
    * เก็บ 1×2 implied (normalize), cross-bookmaker stats
- เขียนขึ้น Firebase:
    matches/{league_id}/{fixture_id}/odds_features  = {...}
- (ออปชัน) บันทึก monitoring summary

CLI:
  --json PATH               (required) ไฟล์จาก af_today_odds.py
  --dry-run                 พิมพ์ preview ไม่เขียน Firebase
  --monitor-path PATH       default=monitoring/odds/last_run
  --bm INT                  (unused inเวอร์ชันนี้, กันไว้อนาคต)

Env for writing:
  FIREBASE_CREDENTIALS  : base64 service account JSON
  FIREBASE_DATABASE_URL : your RTDB/Firestore URL (ตาม fb_client)
"""

import os
import json
import math
import argparse
from statistics import mean, pstdev
from datetime import datetime, timezone

# ---------- FB client (optional import) ----------
def load_fb_update_multi():
    try:
        from fb_client import update_multi  # โปรเจกต์คุณมี retry/backoff แล้ว
        return update_multi
    except Exception as e:
        print("⚠️ fb_client.update_multi not available:", e)
        return None

# ---------- utils ----------
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

def bookmaker_stats_1x2(books):
    """รวมทุกเจ้ามือ → สถิติของ home/draw/away odds + implied เฉลี่ย/overround/entropy"""
    home_odds, draw_odds, away_odds = [], [], []
    pH_list, pD_list, pA_list = [], [], []
    for _, mk in (books or {}).items():
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
        "odds": {"home": agg(home_odds), "draw": agg(draw_odds), "away": agg(away_odds)},
        "implied_avg": {"home": pH, "draw": pD, "away": pA, "overround": ov, "entropy": ent},
    }

# ---------- NEW helpers: balanced line picking & merging all lines ----------
def _imp(odd):
    try:
        o = float(str(odd))
        return 1.0 / o if o > 0 else None
    except:
        return None

def choose_ou_balanced(ou_map: dict, anchor=2.5):
    """
    เลือก line ที่ over/under 'บาลานซ์' สุด (gap น้อย) และใกล้ anchor (2.5) รองลงมา
    ไม่ฟิกซ์ 2.5 — ถ้า 3.0 หรือ 2.25 บาลานซ์กว่า ก็เลือกได้
    """
    if not ou_map: return None, None
    best = None
    for line, v in ou_map.items():
        po, pu = _imp(v.get("over")), _imp(v.get("under"))
        if po is None or pu is None:
            continue
        gap = abs(po - pu)
        cand = (gap, abs(float(line) - float(anchor)), float(line), v)
        if best is None or cand < best:
            best = cand
    if best:
        _, _, line, val = best
        return str(line), val
    # fallback: เลือกใกล้ anchor ที่สุด
    try:
        k = min(ou_map.keys(), key=lambda x: abs(float(x) - float(anchor)))
    except Exception:
        k = list(ou_map.keys())[0]
    return str(k), ou_map[k]

def choose_hcp_balanced(hcp_map: dict):
    """
    เลือก handicap ที่ |line| ใกล้ 0 สุด และราคาบาลานซ์ระหว่าง home/away (gap น้อย)
    ไม่ฟิกซ์ -1 — ถ้า 0, ±0.25, ±0.5 มี ก็จะถูกเลือกก่อน
    """
    if not hcp_map: return None, None
    best = None
    for line, v in hcp_map.items():
        ph, pa = _imp(v.get("home")), _imp(v.get("away"))
        if ph is None or pa is None:
            continue
        cand = (abs(float(line)), abs(ph - pa), float(line), v)
        if best is None or cand < best:
            best = cand
    if best:
        _, _, line, val = best
        return str(line), val
    # fallback: ใกล้ 0 ที่สุด
    try:
        k = min(hcp_map.keys(), key=lambda x: abs(float(x)))
    except Exception:
        k = list(hcp_map.keys())[0]
    return str(k), hcp_map[k]

def merge_all_lines(books: dict, key: str) -> dict:
    """
    รวมทุกเส้นจากทุกเจ้ามือเป็น map รวม (เช่น key='ou', 'hcp', 'ou_ht', 'hcp_ht')
    """
    out = {}
    for mk in (books or {}).values():
        for line, val in (mk.get(key) or {}).items():
            out.setdefault(str(line), val)
    return out

# ---------- core: build features for one fixture ----------
def build_features_per_fixture(rec):
    """
    คืน dict ของ odds_features สำหรับ fixture เดียว (ไม่ fix เส้น)
    - เลือกเส้นหลักแบบ balanced-picked (FT/HT)
    - เก็บทุกเส้น *_all เพื่อให้ downstream ใช้เต็ม
    - เก็บ implied 1×2 + cross-bookmaker stats
    """
    lid = rec["league_id"]; fid = rec["fixture_id"]
    books = rec.get("bookmakers", {}) or {}

    # รวมทุกเส้นจากทุกเจ้า (FT/HT)
    ou_all      = merge_all_lines(books, "ou")
    hcp_all     = merge_all_lines(books, "hcp")
    ou_ht_all   = merge_all_lines(books, "ou_ht")
    hcp_ht_all  = merge_all_lines(books, "hcp_ht")

    # เลือกเส้นหลักแบบบาลานซ์
    ou_line_key,   ou_sel     = choose_ou_balanced(ou_all)       if ou_all     else (None, None)
    hcp_line_key,  hcp_sel    = choose_hcp_balanced(hcp_all)     if hcp_all    else (None, None)
    ou_ht_line,    ou_ht_sel  = choose_ou_balanced(ou_ht_all)    if ou_ht_all  else (None, None)
    hcp_ht_line,   hcp_ht_sel = choose_hcp_balanced(hcp_ht_all)  if hcp_ht_all else (None, None)

    # 1×2 implied baseline (ใช้เจ้ามือแรกที่มี)
    one_any = next((mk.get("1x2") for mk in books.values() if mk.get("1x2")), {})
    def _num(x):
        try: return float(x)
        except: return None
    h = _num(one_any.get("home")); d = _num(one_any.get("draw")); a = _num(one_any.get("away"))
    tot = sum(x for x in (1/h if h else 0, 1/d if d else 0, 1/a if a else 0))
    implied = {
        "home": (1/h / tot) if (h and tot) else None,
        "draw": (1/d / tot) if (d and tot) else None,
        "away": (1/a / tot) if (a and tot) else None,
        "overround": tot if tot else None
    }

    markets_present = sorted({k for _, mk in books.items() for k in mk.keys() if (mk or {}).get(k)})
    xbm = bookmaker_stats_1x2(books)

    features = {
        # FT/HT – เส้นหลัก (balanced-picked)
        "one": one_any,
        "ou":    ({"line": ou_line_key,    **(ou_sel or {})}    if ou_line_key    else {"line": None}),
        "hcp":   ({"line": hcp_line_key,   **(hcp_sel or {})}   if hcp_line_key   else {"line": None}),
        "one_ht": next((mk.get("1x2_ht") for mk in books.values() if mk.get("1x2_ht")), {}) or {},
        "ou_ht": ({"line": ou_ht_line,     **(ou_ht_sel or {})} if ou_ht_line     else {"line": None}),
        "hcp_ht":({"line": hcp_ht_line,    **(hcp_ht_sel or {})}if hcp_ht_line    else {"line": None}),

        # FT/HT – เก็บทุกเส้น
        "ou_all": ou_all,
        "hcp_all": hcp_all,
        "ou_ht_all": ou_ht_all,
        "hcp_ht_all": hcp_ht_all,

        # metrics
        "implied": implied,
        "xbm_1x2": xbm,
        "markets_present": markets_present,
        "n_bookmakers": len(books),
        "n_markets": len(markets_present),
        "flags": {
            "has_1x2": bool(one_any),
            "has_ou":  bool(ou_all),
            "has_hcp": bool(hcp_all),
            "has_1x2_ht": bool(next((mk.get("1x2_ht") for mk in books.values() if mk.get("1x2_ht")), {})),
            "has_ou_ht":  bool(ou_ht_all),
            "has_hcp_ht": bool(hcp_ht_all),
            "complete_ft": bool(one_any and ou_all and hcp_all),
            "complete_ht": bool(ou_ht_all and hcp_ht_all),
        },
        "meta": {"updated_at": now_iso()},
        "_ids": {"league_id": lid, "fixture_id": fid}
    }
    return features

# ---------- preview ----------
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
    ap.add_argument("--bm", type=int, default=0, help="reserved (not used)")
    ap.add_argument("--dry-run", action="store_true", help="preview only, do not write Firebase")
    ap.add_argument("--monitor-path", default="monitoring/odds/last_run", help="Firebase path for run summary")
    args = ap.parse_args()

    with open(args.json, encoding="utf-8") as f:
        payload = json.load(f)

    fixtures = payload.get("fixtures", []) or []
    meta_in = payload.get("meta", {}) or {}
    print(f"อ่านไฟล์: {args.json} | fixtures={len(fixtures)}")

    updates = {}
    n_feat_nodes = 0

    for rec in fixtures:
        lid = int(rec["league_id"])
        fid = int(rec["fixture_id"])

        # เขียน odds_features ต่อแมตช์
        feat = build_features_per_fixture(rec)
        updates[f"matches/{lid}/{fid}/odds_features"] = feat
        n_feat_nodes += 1

        if n_feat_nodes % 50 == 0:
            print(f"เตรียมอัปเดตครบ {n_feat_nodes} fixtures ...")

    # monitoring summary
    summary = {
        "run_at": now_iso(),
        "input_json": os.path.basename(args.json),
        "fixtures": len(fixtures),
        "feature_nodes": n_feat_nodes,
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
