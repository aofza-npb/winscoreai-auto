# fb_client.py
import os, math, time, random, socket
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, db, exceptions as fa_ex

from common_env import get_firebase_cred_dict, get_firebase_db_url

# ===== Tunables via ENV =====
_FB_UPDATE_RETRIES     = int(os.getenv("FB_UPDATE_RETRIES", "5"))
_FB_UPDATE_BASE_SLEEP  = float(os.getenv("FB_UPDATE_BASE_SLEEP", "0.8"))
_FB_UPDATE_MAX_SLEEP   = float(os.getenv("FB_UPDATE_MAX_SLEEP", "8.0"))
_FB_UPDATE_CHUNK_SIZE  = int(os.getenv("FB_UPDATE_CHUNK_SIZE", "2000"))
_FB_UPDATE_JITTER_MAX  = float(os.getenv("FB_UPDATE_JITTER_MAX", "0.5"))

# metrics storage path toggle (None/"" = ปิด)
_FB_METRICS_PATH       = os.getenv("FB_METRICS_PATH", "")  # e.g. "/_ops/metrics"
# continue even if some chunks fail
_FB_CONTINUE_ON_ERROR  = os.getenv("FB_CONTINUE_ON_ERROR", "false").lower() == "true"

def _utc_iso():
    return datetime.now(timezone.utc).isoformat()

def _hostname():
    try: return socket.gethostname()
    except: return "unknown-host"

def _job_context():
    """รวบรวมคอนเท็กซ์จาก CI/โลคอล (ใส่ใน metrics)"""
    return {
        "hostname": _hostname(),
        "pid": os.getpid(),
        "time_utc": _utc_iso(),
        "env": {
            "GITHUB_RUN_ID": os.getenv("GITHUB_RUN_ID"),
            "GITHUB_RUN_NUMBER": os.getenv("GITHUB_RUN_NUMBER"),
            "GITHUB_WORKFLOW": os.getenv("GITHUB_WORKFLOW"),
            "GITHUB_JOB": os.getenv("GITHUB_JOB"),
            "GITHUB_SHA": os.getenv("GITHUB_SHA"),
            "GITHUB_REF": os.getenv("GITHUB_REF"),
            "GITHUB_REPOSITORY": os.getenv("GITHUB_REPOSITORY"),
        }
    }

def init_firebase():
    if firebase_admin._apps:
        return
    cred = credentials.Certificate(get_firebase_cred_dict())
    firebase_admin.initialize_app(cred, {"databaseURL": get_firebase_db_url()})

def ref(path: str):
    init_firebase()
    return db.reference(path)

def get(path: str):
    return ref(path).get()

def set_(path: str, value: Any):
    ref(path).set(value)

def update(path: str, data: Dict[str, Any]):
    ref(path).update(data)

def _sleep_backoff(attempt: int):
    base = min(_FB_UPDATE_MAX_SLEEP, _FB_UPDATE_BASE_SLEEP * (2 ** (attempt - 1)))
    jitter = random.random() * _FB_UPDATE_JITTER_MAX
    time.sleep(base + jitter)

def _update_chunk_with_retry(chunk_items: List[Tuple[str, Any]], chunk_idx: int, total_chunks: int) -> Dict[str, Any]:
    """อัปเดต chunk เดียว พร้อม retry/backoff; คืน metrics ของชิ้นนี้"""
    started = time.time()
    payload = dict(chunk_items)
    attempt_count = 0
    errors: List[str] = []
    while True:
        attempt_count += 1
        try:
            db.reference("/").update(payload)
            dur = time.time() - started
            if total_chunks > 1:
                print(f"✅ Firebase update chunk {chunk_idx+1}/{total_chunks} — keys={len(payload)} in {dur:.2f}s (attempts={attempt_count})")
            return {
                "chunk_index": chunk_idx,
                "keys": len(payload),
                "attempts": attempt_count,
                "duration_s": round(dur, 3),
                "ok": True,
                "error": None,
            }
        except (fa_ex.FirebaseError, Exception) as e:
            err_msg = getattr(e, "message", str(e))
            errors.append(err_msg)
            print(f"⚠️  chunk {chunk_idx+1}/{total_chunks} failed attempt {attempt_count}/{_FB_UPDATE_RETRIES}: {err_msg}")
            if attempt_count >= _FB_UPDATE_RETRIES:
                dur = time.time() - started
                return {
                    "chunk_index": chunk_idx,
                    "keys": len(payload),
                    "attempts": attempt_count,
                    "duration_s": round(dur, 3),
                    "ok": False,
                    "error": errors[-1] if errors else "unknown",
                }
            _sleep_backoff(attempt_count)

def update_multi(
    updates: Dict[str, Any],
    *,
    chunk_size: Optional[int] = None,
    dry_run: bool = False,
    metrics_path: Optional[str] = None,
    continue_on_error: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Multi-location update ที่ root ("/") พร้อม metrics + retry/backoff ต่อ chunk.
    คืน summary dict: {keys_total, chunks_total, chunks_ok, chunks_fail, duration_s, ...}
    ถ้า metrics_path (หรือ ENV FB_METRICS_PATH) ถูกตั้งค่า จะเขียน metrics เข้า Firebase ด้วย
    """
    if not updates:
        msg = "ℹ️ update_multi: no updates to write."
        print(msg)
        return {"message": msg, "keys_total": 0, "chunks_total": 0, "chunks_ok": 0, "chunks_fail": 0}

    init_firebase()
    items = list(updates.items())
    n = len(items)
    cs = chunk_size or _FB_UPDATE_CHUNK_SIZE
    mpath = metrics_path if metrics_path is not None else _FB_METRICS_PATH
    cont = _FB_CONTINUE_ON_ERROR if continue_on_error is None else continue_on_error

    if dry_run:
        preview = dict(items[:min(10, n)])
        print(f"[DRY RUN] update_multi: total keys={n} | chunk_size={cs} | metrics_path={mpath or '-'}")
        for k, v in preview.items():
            print("  ", k, "=>", v)
        return {
            "dry_run": True,
            "keys_total": n,
            "chunks_total": math.ceil(n / cs),
            "chunks_ok": 0,
            "chunks_fail": 0,
        }

    t0 = time.time()
    chunk_metrics: List[Dict[str, Any]] = []

    # one-shot?
    if n <= cs:
        m = _update_chunk_with_retry(items, 0, 1)
        chunk_metrics.append(m)
    else:
        total_chunks = math.ceil(n / cs)
        print(f"🚚 Splitting into {total_chunks} chunks (chunk_size={cs}) — total keys={n}")
        for i in range(total_chunks):
            start = i * cs
            end = min((i + 1) * cs, n)
            chunk_items = items[start:end]
            m = _update_chunk_with_retry(chunk_items, i, total_chunks)
            chunk_metrics.append(m)
            if not m["ok"] and not cont:
                print("⛔ Stopping due to chunk failure (continue_on_error=false).")
                break

    # summary
    dur = time.time() - t0
    chunks_ok = sum(1 for m in chunk_metrics if m["ok"])
    chunks_fail = sum(1 for m in chunk_metrics if not m["ok"])
    retries_used = sum(max(0, m["attempts"] - 1) for m in chunk_metrics)
    summary = {
        "keys_total": n,
        "chunk_size": cs,
        "chunks_total": len(chunk_metrics),
        "chunks_ok": chunks_ok,
        "chunks_fail": chunks_fail,
        "retries_used": retries_used,
        "duration_s": round(dur, 3),
        "time_utc": _utc_iso(),
        "context": _job_context(),
    }

    print(f"📊 update_multi summary → keys={n}, chunks={len(chunk_metrics)} "
          f"(ok={chunks_ok}, fail={chunks_fail}), retries={retries_used}, took {dur:.2f}s")

    # push metrics to Firebase?
    if mpath:
        try:
            job_id = os.getenv("GITHUB_RUN_ID") or f"local-{int(time.time())}"
            date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            node_base = f"{mpath.rstrip('/')}/{date_key}/{job_id}"
            # เก็บ summary
            db.reference(node_base).set(summary)
            # เก็บรายละเอียด per-chunk (จำกัด 200 รายการ)
            details = {str(m["chunk_index"]): m for m in chunk_metrics[:200]}
            db.reference(f"{node_base}/chunks").set(details)
            print(f"📝 metrics written to Firebase at {node_base}")
        except Exception as e:
            print(f"⚠️  failed to write metrics to Firebase: {e}")

    return summary

def patch_merge(path: str, patch_obj: Dict[str, Any]):
    cur = get(path) or {}
    cur.update(patch_obj or {})
    set_(path, cur)
