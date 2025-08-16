
# af_today_odds.py
# ดึง odds ตลาด 1X2, Over/Under, Asian Handicap → บันทึก JSON + CSV
import os, csv, argparse, time, re, json
from datetime import datetime, timedelta
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")
assert API_KEY, "ยังไม่พบ API_FOOTBALL_KEY (ตั้งใน .env หรือ setx มาก่อน)"

BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# API-Football bet ids:
BET_MATCH_WINNER = 1      # 1X2
BET_OVER_UNDER   = 2      # O/U
BET_HANDICAP     = 5      # Asian Handicap

SUPPORTED_BETS = {BET_MATCH_WINNER, BET_OVER_UNDER, BET_HANDICAP}

def get(url, params, what="", max_retry=3, wait=1.5):
    for i in range(max_retry):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            j = r.json()
            return j.get("response", []), j.get("errors", {})
        # 429/5xx หรืออื่นๆ → รอแล้ว retry
        time.sleep(wait * (1 + 0.5*i))
    raise RuntimeError(f"GET {what or url} failed after retries")

def read_allowlist(path):
    lids = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            parts = s.split()
            if parts[0].isdigit():
                lids.append(int(parts[0]))
    return lids

def status_long(rec):
    return rec.get("fixture", {}).get("status", {}).get("long")

def parse_ou_value(v: str):
    # "Over 2.5" / "Under 3" / บางกรณีเป็น "Over 2.25"
    m = re.match(r"^(Over|Under)\s+([0-9]+(?:\.[0-9]+)?)$", str(v).strip(), flags=re.I)
    if not m:
        return None, None
    side = m.group(1).title()  # Over/Under
    line = m.group(2)
    return side, line

def parse_hcp_value(v: str):
    # "Home -1" / "Away +0.25" / บางกรณีเป็น "-1.25 Home"
    s = str(v).strip()
    m1 = re.match(r"^(Home|Away)\s+([+-]?[0-9]+(?:\.[0-9]+)?)$", s, flags=re.I)
    m2 = re.match(r"^([+-]?[0-9]+(?:\.[0-9]+)?)\s+(Home|Away)$", s, flags=re.I)
    if m1:
        side = m1.group(1).title()   # Home/Away
        line = m1.group(2)
        return side, line
    if m2:
        side = m2.group(2).title()
        line = m2.group(1)
        return side, line
    # บางครั้งค่ามาเป็น "0" อย่างเดียว → ถือเป็นเสมอขาว (0) ให้เป็น Home/ Away ต้องแยกไม่ได้
    if re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", s):
        return None, s
    return None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.utcnow().strftime("%Y-%m-%d"),
                    help="YYYY-MM-DD (default=today, UTC)")
    ap.add_argument("--days", type=int, default=1,
                    help="ดึงกี่วัน (เช่น 2 = วันนี้+พรุ่งนี้)")
    ap.add_argument("--allow", default="allowlist_A.txt",
                    help="ไฟล์รายลีก (คอลัมน์แรกคือ league_id)")
    ap.add_argument("--bookmaker", type=int, default=6,
                    help="เช่น 6 = Bet365 (ถ้าไม่ระบุ จะเก็บทุกเจ้า)")
    ap.add_argument("--outdir", default="live_odds", help="โฟลเดอร์เอาต์พุต")
    ap.add_argument("--sleep", type=float, default=0.25, help="หน่วงต่อ fixture (กันเรตลิมิต)")
    args = ap.parse_args()

    lids = set(read_allowlist(args.allow))
    start = datetime.strptime(args.date, "%Y-%m-%d").date()
    end = start + timedelta(days=args.days-1)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    print(f"ดึงแมตช์ + odds [{args.date} → {end}] | leagues={len(lids)} | bookmaker={args.bookmaker or 'ALL'}")

    # โครงสร้าง JSON รวม
    # fixtures_map[(league_id, fixture_id)] = {... fixture info ..., "bookmakers": {bm_id: {...markets...}}}
    fixtures_map = {}

    # CSV rows (normalized)
    csv_rows = []  # season,date,league_id,fixture_id,home,away,bookmaker_id,market,selection,line,odd

    for d in (start + timedelta(days=i) for i in range(args.days)):
        ds = d.strftime("%Y-%m-%d")

        # 1) ดึง fixtures รายวัน
        fx, _ = get(f"{BASE}/fixtures", {"date": ds}, what=f"fixtures {ds}")

        # คัดเฉพาะลีกที่ต้องการ + เฉพาะแมตช์ที่ยังไม่เริ่ม
        fx = [
            x for x in fx
            if int(str(x.get("league", {}).get("id", "-1")) or -1) in lids
            and status_long(x) in ("Not Started", "Time to be defined")
        ]
        print(f"• {ds} fixtures ก่อนเตะ: {len(fx)}")

        # เตรียมคำขอ odds ต่อ fixture
        for f in fx:
            fid = int(f.get("fixture", {}).get("id"))
            lid = int(f.get("league", {}).get("id"))
            home = f.get("teams", {}).get("home", {}).get("name", "")
            away = f.get("teams", {}).get("away", {}).get("name", "")
            season = int(f.get("league", {}).get("season", 0) or 0)

            # init node ใน JSON รวม
            key = (lid, fid)
            if key not in fixtures_map:
                fixtures_map[key] = {
                    "season": season,
                    "date": ds,
                    "league_id": lid,
                    "fixture_id": fid,
                    "home": home,
                    "away": away,
                    "bookmakers": {}  # bm_id -> { "1x2": {...}, "ou": {line: {Over,Under}}, "hcp": {line: {Home,Away}} }
                }

            params = {"fixture": fid}
            if args.bookmaker:
                params["bookmaker"] = args.bookmaker

            odds, _ = get(f"{BASE}/odds", params, what=f"odds fid={fid}")
            # odds response อาจมีหลาย entry → loop รวมทั้งหมด
            if odds:
                for entry in odds:
                    for bm in entry.get("bookmakers", []):
                        bm_id = bm.get("id")
                        if not bm_id:
                            continue
                        bm_id = int(bm_id)
                        bnode = fixtures_map[key]["bookmakers"].setdefault(
                            str(bm_id),
                            {"1x2": {}, "ou": {}, "hcp": {}}
                        )

                        for bet in bm.get("bets", []):
                            bet_id = bet.get("id")
                            if bet_id not in SUPPORTED_BETS:
                                continue

                            # --- 1X2 ---
                            if bet_id == BET_MATCH_WINNER:
                                rec = {"Home": "", "Draw": "", "Away": ""}
                                for v in bet.get("values", []):
                                    val = v.get("value")   # Home/Draw/Away
                                    odd = v.get("odd")
                                    if val in rec:
                                        rec[val] = odd

                                # JSON รวม
                                # ช่องว่างไม่ต้องกรอกหากไม่มี
                                if rec.get("Home") or rec.get("Draw") or rec.get("Away"):
                                    bnode["1x2"] = {
                                        "home": rec["Home"],
                                        "draw": rec["Draw"],
                                        "away": rec["Away"]
                                    }

                                # CSV (3 แถว)
                                for sel_key, sel_name in [("Home","Home"),("Draw","Draw"),("Away","Away")]:
                                    if rec[sel_key]:
                                        csv_rows.append([
                                            season, ds, lid, fid, home, away,
                                            bm_id, "1X2", sel_name, "", rec[sel_key]
                                        ])

                            # --- Over/Under ---
                            elif bet_id == BET_OVER_UNDER:
                                for v in bet.get("values", []):
                                    val = v.get("value")   # "Over 2.5" / "Under 2.5"
                                    odd = v.get("odd")
                                    side, line = parse_ou_value(val or "")
                                    if not line or side not in ("Over","Under"):
                                        continue

                                    # JSON รวม
                                    ou_line = bnode["ou"].setdefault(line, {"Over": "", "Under": ""})
                                    ou_line[side] = odd

                                # เติม CSV ทีหลังเมื่อเรามีบัคเก็ตครบทุก values แล้ว
                                for line, od in bnode["ou"].items():
                                    if od.get("Over"):
                                        csv_rows.append([
                                            season, ds, lid, fid, home, away,
                                            bm_id, "O/U", "Over", line, od["Over"]
                                        ])
                                    if od.get("Under"):
                                        csv_rows.append([
                                            season, ds, lid, fid, home, away,
                                            bm_id, "O/U", "Under", line, od["Under"]
                                        ])

                            # --- Handicap ---
                            elif bet_id == BET_HANDICAP:
                                for v in bet.get("values", []):
                                    val = v.get("value")   # "Home -1" / "Away +0.25" / "0"
                                    odd = v.get("odd")
                                    side, line = parse_hcp_value(val or "")
                                    if not line:
                                        continue
                                    hcp_line = bnode["hcp"].setdefault(line, {"Home": "", "Away": ""})
                                    if side in ("Home","Away"):
                                        hcp_line[side] = odd

                                for line, od in bnode["hcp"].items():
                                    if od.get("Home"):
                                        csv_rows.append([
                                            season, ds, lid, fid, home, away,
                                            bm_id, "HCP", "Home", line, od["Home"]
                                        ])
                                    if od.get("Away"):
                                        csv_rows.append([
                                            season, ds, lid, fid, home, away,
                                            bm_id, "HCP", "Away", line, od["Away"]
                                        ])

            else:
                # ไม่มี odds → แปะแถวแจ้งสถานะ (สำหรับ CSV; JSON ไม่มีอะไรเพิ่ม)
                csv_rows.append([season, ds, lid, fid, home, away,
                                 args.bookmaker or "", "NONE", "", "", ""])

            # กันเรตลิมิต
            if args.sleep > 0:
                time.sleep(args.sleep)

    # เขียนไฟล์ JSON + CSV
    json_path = outdir / f"odds_full_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.json"
    csv_path  = outdir / f"odds_full_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.csv"

    # JSON: แปลง map → list
    json_list = []
    for (lid, fid), node in fixtures_map.items():
        json_list.append(node)

    with open(json_path, "w", encoding="utf-8") as wj:
        json.dump(json_list, wj, ensure_ascii=False, indent=2)

    with open(csv_path, "w", newline="", encoding="utf-8") as wc:
        wr = csv.writer(wc)
        wr.writerow([
            "season","date","league_id","fixture_id","home","away",
            "bookmaker_id","market","selection","line","odd"
        ])
        wr.writerows(csv_rows)

    print(f"✅ เขียนไฟล์ JSON: {json_path} (fixtures={len(json_list)})")
    print(f"✅ เขียนไฟล์ CSV : {csv_path} (rows={len(csv_rows)})")
    if json_list[:1]:
        print("ตัวอย่าง JSON (รายการแรก):")
        print(json.dumps(json_list[0], ensure_ascii=False, indent=2)[:800])

if __name__ == "__main__":
    main()
