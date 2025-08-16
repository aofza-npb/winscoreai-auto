# API-Football/scripts/patch_odds.py
import os, sys, json, glob, argparse
from datetime import datetime, timezone
from typing import Dict, Any

from fb_client import update_multi
from firebase_admin import db

DEFAULT_LIVE_DIR = os.path.join(os.path.dirname(__file__), "..", "live_odds")

def find_latest_json(live_dir: str) -> str:
    pats = sorted(glob.glob(os.path.join(live_dir, "odds_full_*.json")))
    if not pats:
        raise FileNotFoundError(f"ไม่พบไฟล์ odds_full_*.json ใน {live_dir}")
    return max(pats, key=os.path.getmtime)

def load_json(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("รูปแบบ JSON ควรเป็นลิสต์ของ fixtures (list[object])")
    return data

def safe_str(x) -> str:
    return "" if x is None else str(x)

def add_meta(updates: Dict[str, Any], base: str, bookmaker_id: int, ts_iso: str):
    updates[f"{base}/meta/source"] = "api-sports-v3"
    updates[f"{base}/meta/bookmaker_id"] = bookmaker_id
    updates[f"{base}/meta/updated_at"] = ts_iso

def patch_fixture_bookmaker(updates: Dict[str, Any], node: dict, bm_id: int, ts_iso: str):
    """
    เติมข้อมูลของเจ้ามือรายหนึ่งลงใน dict 'updates' เพื่อทำ multi-location update ครั้งเดียว
    """
    league_id = node["league_id"]
    fixture_id = node["fixture_id"]
    base = f"matches/{league_id}/{fixture_id}/odds/{bm_id}"

    # 1X2
    m1x2 = (node.get("bookmakers", {}).get(str(bm_id), {}) or {}).get("1x2", {}) or {}
    if any(safe_str(m1x2.get(k)) for k in ("home","draw","away")):
        # เขียนเฉพาะฟิลด์ที่มีค่า
        if safe_str(m1x2.get("home")):
            updates[f"{base}/1x2/home"] = safe_str(m1x2.get("home"))
        if safe_str(m1x2.get("draw")):
            updates[f"{base}/1x2/draw"] = safe_str(m1x2.get("draw"))
        if safe_str(m1x2.get("away")):
            updates[f"{base}/1x2/away"] = safe_str(m1x2.get("away"))

    # O/U
    ou = (node.get("bookmakers", {}).get(str(bm_id), {}) or {}).get("ou", {}) or {}
    for line, obj in ou.items():
        line_s = safe_str(line)
        if not line_s:
            continue
        over_v = safe_str(obj.get("Over"))
        under_v = safe_str(obj.get("Under"))
        if over_v:
            updates[f"{base}/ou/{line_s}/Over"] = over_v
        if under_v:
            updates[f"{base}/ou/{line_s}/Under"] = under_v

    # Handicap
    hcp = (node.get("bookmakers", {}).get(str(bm_id), {}) or {}).get("hcp", {}) or {}
    for line, obj in hcp.items():
        line_s = safe_str(line)
        if not line_s:
            continue
        home_v = safe_str(obj.get("Home"))
        away_v = safe_str(obj.get("Away"))
        if home_v:
            updates[f"{base}/hcp/{line_s}/Home"] = home_v
        if away_v:
            updates[f"{base}/hcp/{line_s}/Away"] = away_v

    # meta (อัปเดตทุกครั้งเพื่อ timestamp ล่าสุด)
    add_meta(updates, base, bm_id, ts_iso)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="พาธไฟล์ odds_full_*.json (ถ้าไม่ระบุ จะใช้ไฟล์ล่าสุดใน live_odds/)")
    ap.add_argument("--live_dir", default=DEFAULT_LIVE_DIR, help="โฟลเดอร์ live_odds")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-bookmaker", type=str, default="", help="คอมมาไอดีเจ้ามือที่ต้องการแพตช์ เช่น '6,8' (ว่าง=ทุกเจ้า)")
    args = ap.parse_args()

    json_path = args.json or find_latest_json(args.live_dir)
    fixtures = load_json(json_path)
    print(f"อ่าน JSON: {json_path} | fixtures={len(fixtures)}")

    # ฟิลเตอร์รายเจ้ามือ (ถ้าระบุ)
    bm_filter = set()
    if args.only_bookmaker.strip():
        bm_filter = {s.strip() for s in args.only_bookmaker.split(",") if s.strip()}

    ts_iso = datetime.now(timezone.utc).isoformat()

    # เตรียม updates รวม (multi-location)
    updates: Dict[str, Any] = {}
    total_pairs = 0
    total_markets = 0

    for fx in fixtures:
        # ข้อมูลหลัก
        league_id = fx.get("league_id")
        fixture_id = fx.get("fixture_id")
        bms = fx.get("bookmakers", {}) or {}
        if not league_id or not fixture_id or not bms:
            # ไม่มีเจ้ามือใน fixture นี้ → ติด tag missing ที่ odds/-1/meta (ครั้งนี้ขอข้ามเพื่อไม่ทับของเดิม)
            # ถ้าต้องการ mark missing ให้ปลดคอมเมนต์ด้านล่าง
            # base = f"matches/{league_id}/{fixture_id}/odds/-1"
            # add_meta(updates, base, -1, ts_iso)
            continue

        for bm_id_str, markets in bms.items():
            if bm_filter and bm_id_str not in bm_filter:
                continue

            try:
                bm_id = int(bm_id_str)
            except Exception:
                # skip id ที่ไม่ใช่ตัวเลข
                continue

            # บันทึกตลาดลง updates
            before = len(updates)
            patch_fixture_bookmaker(updates, fx, bm_id, ts_iso)
            after = len(updates)
            if after > before:
                total_pairs += 1
                # ประมาณการจำนวนตลาดจากจำนวนคีย์ที่เพิ่ม
                total_markets += (after - before)

    if not updates:
        print("ไม่มีข้อมูลใหม่ให้แพตช์")
        return

    print(f"เตรียมอัปเดต {total_pairs} fixture-bookmaker pairs | keys={total_markets}")
    if args.dry_run:
        # แสดงตัวอย่าง 10 แถวแรกเพื่อความชัวร์
        print("---- DRY RUN (ตัวอย่าง 10 updates) ----")
        for i, (k, v) in enumerate(updates.items()):
            if i >= 10: break
            print(k, "=>", v)
        print("---- END DRY ----")
        return

    # เริ่มเขียนจริง
    #init_firebase()
    #db.reference("/").update(updates)
    update_multi(updates)
    print("✅ PATCH สำเร็จ (multi-location update)")

if __name__ == "__main__":
    main()
