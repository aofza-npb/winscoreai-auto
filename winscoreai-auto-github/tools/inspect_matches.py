# tools/inspect_matches.py
import os, json, re
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import firebase_admin
from firebase_admin import credentials, db

DBURL = "https://winscoreai-app-default-rtdb.asia-southeast1.firebasedatabase.app/"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "14"))

# สเปคขั้นต่ำที่ตรวจ
MUST_PATHS = [
    "date", "season",
    "teams.home.name", "teams.away.name",
]
SHOULD_PATHS = [
    "teams.home.id", "teams.away.id",
    "league.id", "league.name",
    # ตลาด (ตรวจเป็น group ด้านล่าง)
]
MARKET_GROUPS = {
    "1x2": ["odds_features.1x2.home", "odds_features.1x2.draw", "odds_features.1x2.away"],
    "asian": ["odds_features.asian.handicap", "odds_features.asian.home_odds", "odds_features.asian.away_odds"],
    "totals": ["odds_features.totals.line", "odds_features.totals.over", "odds_features.totals.under"],
}
NICE_PATHS = [
    "score.ft.home", "score.ft.away",
    "score.ht.home", "score.ht.away",
    "xg.home", "xg.away",
]

def init_fb():
    if not firebase_admin._apps:
        key = os.environ["FIREBASE_ADMIN_KEY"]
        cred = credentials.Certificate(json.loads(key))
        firebase_admin.initialize_app(cred, {"databaseURL": DBURL})

def _dget(d, path):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur: return None
        cur = cur[p]
    return cur

def unwrap(node):
    # บางโปรเจคห่อด้วย results
    return node.get("results", node) if isinstance(node, dict) else {}

def fetch_matches():
    ref = db.reference("matches")
    data = ref.get() or {}
    items = []
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).date()
    for season, fixtures in (data or {}).items():
        if not isinstance(fixtures, dict): continue
        for fid, node in fixtures.items():
            body = unwrap(node)
            date = body.get("date")
            try:
                d = datetime.fromisoformat(date).date()
            except Exception:
                continue
            if d >= cutoff:
                items.append((str(fid), body))
    return items

def main():
    init_fb()
    rows = fetch_matches()
    total = len(rows)
    print(f"Scanning fixtures (last {LOOKBACK_DAYS} days): {total}")

    stats = { "must": Counter(), "should": Counter(), "nice": Counter() }
    missing_examples = defaultdict(list)
    market_ok = 0

    for fid, r in rows:
        # MUST & SHOULD & NICE
        for p in MUST_PATHS:
            v = _dget(r, p)
            if v is not None: stats["must"][p] += 1
            else:
                if len(missing_examples[p]) < 5: missing_examples[p].append(fid)

        for p in SHOULD_PATHS:
            v = _dget(r, p)
            if v is not None: stats["should"][p] += 1
            else:
                if len(missing_examples[p]) < 5: missing_examples[p].append(fid)

        for p in NICE_PATHS:
            v = _dget(r, p)
            if v is not None: stats["nice"][p] += 1

        # ตลาด: ขอให้มีอย่างน้อยหนึ่งกลุ่มครบ
        has_market = False
        for g, paths in MARKET_GROUPS.items():
            if all(_dget(r, p) is not None for p in paths):
                has_market = True
                break
        if has_market: market_ok += 1
        else:
            if len(missing_examples["markets"]) < 5:
                missing_examples["markets"].append(fid)

    def pct(x): return round(100.0 * x / total, 1) if total else 0.0

    print("\n=== COVERAGE (MUST) ===")
    for p in MUST_PATHS:
        print(f"{p:30s} : {pct(stats['must'][p])}%")

    print("\n=== COVERAGE (SHOULD) ===")
    for p in SHOULD_PATHS:
        print(f"{p:30s} : {pct(stats['should'][p])}%")

    print("\n=== MARKETS (≥1 group complete) ===")
    print(f"market_ok              : {pct(market_ok)}%")

    print("\n=== COVERAGE (NICE) ===")
    for p in NICE_PATHS:
        print(f"{p:30s} : {pct(stats['nice'][p])}%")

    print("\n=== MISSING EXAMPLES (first 5) ===")
    for k, arr in missing_examples.items():
        print(f"{k}: {arr}")

    # เกณฑ์ fail: MUST < 99% หรือ ไม่มีตลาดครบ ≥1 กลุ่ม < 70%
    must_ok = all(stats["must"][p] >= max(1, int(0.99 * total)) for p in MUST_PATHS)
    market_enough = market_ok >= int(0.70 * total)
    if not must_ok or not market_enough:
        raise SystemExit("❌ Schema coverage too low. Fix matches before features/predictions.")

if __name__ == "__main__":
    main()
