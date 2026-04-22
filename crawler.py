#!/usr/bin/env python3
"""
THD Distributed Link Collector — v2 with Smart Rate Limit Handling
===================================================================
Key improvement: detects 206 rate limit, auto-pauses until IP recovers,
then resumes. Single IP does ~180 req → pause 10-20 min → repeat.
Runs continuously through multiple shards with --auto.

Usage:
    pip install curl_cffi

    python crawler.py --auto                 # claim shards and run until all done
    python crawler.py --auto --max-shards 5  # run up to 5 shards then stop
    python crawler.py --shard 001            # run specific shard
    python crawler.py --list                 # show progress
    python crawler.py --shard 001 --dry-run
"""

import argparse
import json
import logging
import os
import random
import re
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("Missing dependency: pip install curl_cffi")
    sys.exit(1)

ROOT        = Path(__file__).resolve().parent
TASKS_DIR   = ROOT / "tasks"
SHARDS_DIR  = TASKS_DIR / "shards"
CLAIMS_DIR  = TASKS_DIR / "claims"
RESULTS_DIR = TASKS_DIR / "results"
LINKS_DIR   = TASKS_DIR / "links"

GRAPHQL_URL = "https://www.homedepot.com/federation-gateway/graphql"
IMAGE_SIZE  = 1000

# --- Rate limit tuning ---
BURST_SIZE     = 180    # requests before proactive pause (HD triggers 206 at ~200)
BURST_DELAY    = 2.5    # seconds between requests in a burst
COOLDOWN_PROBE = 120    # seconds between probe attempts during cooldown
COOLDOWN_MAX   = 1800   # max cooldown (30 min)
JITTER         = 0.5    # ± random jitter on delays

API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-experience-name": "general-merchandise",
    "Origin": "https://www.homedepot.com",
    "Referer": "https://www.homedepot.com/",
}

PRODUCT_MEDIA_QUERY = """query productClientOnlyProduct($itemId: String!) {
  product(itemId: $itemId) {
    itemId
    identifiers { productLabel brandName modelNumber }
    media { images { url type subType sizes } }
  }
}"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate Limiter — the core anti-206 logic
# ---------------------------------------------------------------------------

class RateLimiter:
    """Tracks request budget per IP. Pauses proactively before 206,
    and reactively recovers if 206 is hit."""

    def __init__(self):
        self.requests_in_burst = 0
        self.total_requests = 0
        self.total_206 = 0
        self.cooldowns = 0

    def pre_request(self):
        """Called before each request. Proactively pauses if near limit."""
        if self.requests_in_burst >= BURST_SIZE:
            self._proactive_cooldown()

    def post_request(self, status_code):
        """Called after request. Returns True if OK."""
        self.total_requests += 1
        if status_code == 200:
            self.requests_in_burst += 1
            return True
        elif status_code == 206:
            self.total_206 += 1
            self._reactive_cooldown()
            return False
        return False

    def _proactive_cooldown(self):
        """Pause BEFORE hitting rate limit. Shorter wait."""
        pause = random.uniform(600, 900)  # 10-15 min
        self.cooldowns += 1
        log.info(
            f"💤 Proactive pause after {self.requests_in_burst} requests. "
            f"Sleeping {pause/60:.0f} min. "
            f"(Total: {self.total_requests} req, {self.cooldowns} pauses)"
        )
        time.sleep(pause)
        self.requests_in_burst = 0

    def _reactive_cooldown(self):
        """Got 206. Wait until IP recovers, probing periodically."""
        log.warning(
            f"🔴 Rate limited (206) at {self.requests_in_burst} requests. "
            f"Probing every {COOLDOWN_PROBE}s until recovered..."
        )
        self.requests_in_burst = 0
        waited = 0
        probe_session = cffi_requests.Session()

        while waited < COOLDOWN_MAX:
            time.sleep(COOLDOWN_PROBE)
            waited += COOLDOWN_PROBE
            try:
                resp = probe_session.post(
                    f"{GRAPHQL_URL}?opname=productClientOnlyProduct",
                    headers=API_HEADERS,
                    json={
                        "operationName": "productClientOnlyProduct",
                        "variables": {"itemId": "203164241"},
                        "query": PRODUCT_MEDIA_QUERY,
                    },
                    impersonate="chrome", timeout=10,
                )
                if resp.status_code == 200:
                    log.info(f"🟢 Recovered after {waited/60:.0f} min!")
                    return
                log.info(f"  Still 206... ({waited/60:.0f} min)")
            except Exception:
                log.info(f"  Probe failed... ({waited/60:.0f} min)")

        log.warning(f"⚠️  Not recovered after {COOLDOWN_MAX/60:.0f} min. Continuing anyway.")

    def stats_dict(self):
        return {
            "total_requests": self.total_requests,
            "total_206": self.total_206,
            "cooldowns": self.cooldowns,
        }


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def git(*args, check=True):
    r = subprocess.run(["git", "-C", str(ROOT)] + list(args),
                       capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(r.stderr.strip())
    return r.stdout.strip(), r.returncode


def git_pull():
    try:
        out, _ = git("pull", "--rebase")
        log.info(f"git pull: {out.splitlines()[0] if out else 'ok'}")
    except RuntimeError as e:
        log.warning(f"git pull failed (continuing): {e}")


def git_push_claim(shard_id):
    CLAIMS_DIR.mkdir(parents=True, exist_ok=True)
    claim_path = CLAIMS_DIR / f"shard_{shard_id}.json"
    claim_path.write_text(json.dumps({
        "shard_id":   shard_id,
        "machine":    socket.gethostname(),
        "pid":        os.getpid(),
        "claimed_at": datetime.now().isoformat(),
    }, indent=2))
    try:
        git("add", str(claim_path))
        git("commit", "-m", f"claim shard {shard_id} [{socket.gethostname()}]")
        git("push")
        log.info(f"Claimed shard {shard_id}")
        return True
    except RuntimeError:
        log.info(f"Shard {shard_id} taken, trying next...")
        git("reset", "HEAD~1", check=False)
        claim_path.unlink(missing_ok=True)
        git("pull", "--rebase", check=False)
        return False


def git_push_result(shard_id, paths):
    try:
        for p in paths:
            git("add", str(p))
        git("commit", "-m", f"done shard {shard_id} [{socket.gethostname()}]")
        git("push")
    except RuntimeError as e:
        log.warning(f"Could not push result: {e}")


# ---------------------------------------------------------------------------
# Auto-claim
# ---------------------------------------------------------------------------

def find_unclaimed_shard():
    git_pull()
    manifest = json.loads((TASKS_DIR / "manifest.json").read_text())
    claimed  = {f.stem for f in CLAIMS_DIR.glob("shard_*.json")} if CLAIMS_DIR.exists() else set()
    done     = {f"shard_{f.stem.replace('_done','').replace('shard_','')}"
                for f in RESULTS_DIR.glob("shard_*_done.json")} if RESULTS_DIR.exists() else set()
    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        if sid not in claimed and sid not in done:
            return s["shard_id"]
    return None


def auto_claim():
    for _ in range(10):
        shard_id = find_unclaimed_shard()
        if shard_id is None:
            log.info("No unclaimed shards — all done!")
            return None
        if git_push_claim(shard_id):
            return shard_id
        time.sleep(2)
    log.error("Could not claim a shard after 10 attempts")
    return None


# ---------------------------------------------------------------------------
# GraphQL fetch
# ---------------------------------------------------------------------------

def graphql_fetch(session, omsid, rate_limiter):
    """Fetch product data. Rate limiter handles 206 automatically."""
    payload = {
        "operationName": "productClientOnlyProduct",
        "variables": {"itemId": omsid},
        "query": PRODUCT_MEDIA_QUERY,
    }

    rate_limiter.pre_request()

    for attempt in range(3):
        try:
            r = session.post(
                f"{GRAPHQL_URL}?opname=productClientOnlyProduct",
                headers=API_HEADERS, json=payload,
                impersonate="chrome", timeout=20,
            )
        except Exception as e:
            return [], None, str(e)

        if r.status_code == 206:
            rate_limiter.post_request(206)
            continue  # rate limiter already waited; retry

        rate_limiter.post_request(r.status_code)

        if r.status_code != 200:
            return [], None, f"HTTP {r.status_code}"

        data    = r.json()
        product = (data.get("data") or {}).get("product")
        if not product:
            err = (data.get("errors") or [{}])[0].get("message", "no product data")
            return [], None, err

        ident = product.get("identifiers", {})
        info  = {
            "brand": ident.get("brandName", ""),
            "model": ident.get("modelNumber", ""),
            "label": ident.get("productLabel", ""),
        }

        images = []
        for img in (product.get("media") or {}).get("images", []):
            url_tmpl = img.get("url", "")
            if not url_tmpl:
                continue
            sizes   = img.get("sizes", [])
            numeric = [s for s in sizes if str(s).isdigit()] if isinstance(sizes, list) else []
            best    = str(max(numeric, key=int)) if numeric else str(IMAGE_SIZE)
            images.append({
                "img_url":    url_tmpl.replace("<SIZE>", best),
                "img_type":   img.get("type", ""),
                "img_subtype": img.get("subType", ""),
            })

        return images, info, None

    return [], None, "max retries"


# ---------------------------------------------------------------------------
# Shard runner
# ---------------------------------------------------------------------------

def run_shard(shard_id, dry_run=False, rate_limiter=None):
    shard_file  = SHARDS_DIR / f"shard_{shard_id}.json"
    result_file = RESULTS_DIR / f"shard_{shard_id}_done.json"

    if not shard_file.exists():
        log.error(f"Shard file not found: {shard_file}")
        sys.exit(1)
    if result_file.exists():
        log.info(f"Shard {shard_id} already completed.")
        return rate_limiter

    shard    = json.loads(shard_file.read_text())
    products = shard["products"]
    log.info(f"=== Shard {shard_id}/{shard['total_shards']:03d} | {len(products)} products (links only) ===")

    if dry_run:
        log.info("[DRY RUN] first 5 products:")
        for p in products[:5]:
            log.info(f"  {p['omsid']} ({p.get('category','?')}) — {p['url']}")
        return rate_limiter

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LINKS_DIR.mkdir(parents=True, exist_ok=True)

    links_file = LINKS_DIR / f"shard_{shard_id}_links.jsonl"
    if rate_limiter is None:
        rate_limiter = RateLimiter()

    # --- Resume: read already-collected omsids ---
    processed = set()
    existing_links = 0
    if links_file.exists():
        with links_file.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    processed.add(json.loads(line)["omsid"])
                    existing_links += 1
                except Exception:
                    pass
        if processed:
            log.info(f"Resume: {len(processed)} products done ({existing_links} links). Continuing...")

    session = cffi_requests.Session()
    stats = {
        "shard_id":          shard_id,
        "machine":           socket.gethostname(),
        "started_at":        datetime.now().isoformat(),
        "products_total":    len(products),
        "products_ok":       len(processed),
        "products_failed":   0,
        "products_no_media": 0,
        "links_collected":   existing_links,
    }
    consecutive_errors = 0

    with links_file.open("a", encoding="utf-8") as f:
        for i, product in enumerate(products):
            omsid = product["omsid"]
            if omsid in processed:
                continue

            page_url = product.get("url", f"https://www.homedepot.com/p/{omsid}")
            category = product.get("category", "other")

            if (i + 1) % 50 == 0:
                log.info(
                    f"[{i+1}/{len(products)}] {(i+1)/len(products)*100:.0f}% | "
                    f"{stats['links_collected']} links | "
                    f"burst={rate_limiter.requests_in_burst}/{BURST_SIZE} | "
                    f"206s={rate_limiter.total_206} | pauses={rate_limiter.cooldowns}"
                )

            images, info, err = graphql_fetch(session, omsid, rate_limiter)

            if err:
                consecutive_errors += 1
                stats["products_failed"] += 1
                if consecutive_errors >= 50:
                    log.error("50 consecutive errors — aborting shard.")
                    break
                if "max retries" not in str(err):
                    log.warning(f"  FAIL {omsid}: {err[:80]}")
                delay = BURST_DELAY + random.uniform(-JITTER, JITTER)
                time.sleep(max(0.5, delay))
                continue

            consecutive_errors = 0
            if not images:
                stats["products_no_media"] += 1
                time.sleep(BURST_DELAY + random.uniform(-JITTER, JITTER))
                continue

            for img in images:
                row = {
                    "omsid":    omsid,
                    "page_url": page_url,
                    "category": category,
                    "brand":    info.get("brand", ""),
                    "model":    info.get("model", ""),
                    "label":    info.get("label", ""),
                    **img,
                }
                f.write(json.dumps(row) + "\n")
                stats["links_collected"] += 1
            f.flush()
            os.fsync(f.fileno())
            processed.add(omsid)
            stats["products_ok"] += 1
            time.sleep(BURST_DELAY + random.uniform(-JITTER, JITTER))

    stats.update({
        "finished_at": datetime.now().isoformat(),
        "status": "done",
        **rate_limiter.stats_dict(),
    })
    result_file.write_text(json.dumps(stats, indent=2))
    git_push_result(shard_id, [result_file, links_file])

    log.info("=" * 55)
    log.info(f"SHARD {shard_id} DONE — {stats['links_collected']} links")
    log.info(f"  Rate: {rate_limiter.total_206} 206s, {rate_limiter.cooldowns} cooldowns")
    log.info("=" * 55)
    return rate_limiter


# ---------------------------------------------------------------------------
# Progress list
# ---------------------------------------------------------------------------

def list_shards():
    git_pull()
    manifest = json.loads((TASKS_DIR / "manifest.json").read_text())
    claimed  = {}
    if CLAIMS_DIR.exists():
        for f in CLAIMS_DIR.glob("shard_*.json"):
            try:
                claimed[f.stem] = json.loads(f.read_text()).get("machine", "?")
            except Exception:
                claimed[f.stem] = "?"

    done_stats = {}
    if RESULTS_DIR.exists():
        for f in RESULTS_DIR.glob("shard_*_done.json"):
            try:
                d = json.loads(f.read_text())
                key = f"shard_{d['shard_id']}"
                done_stats[key] = d.get("links_collected", 0)
            except Exception:
                pass

    total   = manifest["n_shards"]
    n_done  = len(done_stats)
    n_run   = sum(1 for k in claimed if k not in done_stats)
    total_links = sum(done_stats.values())

    print(f"\nProgress: {n_done}/{total} done | {n_run} running | {total-n_done-n_run} pending | {total_links:,} links collected\n")
    print(f"{'Shard':<8} {'Products':<10} {'Status':<30}")
    print("-" * 50)
    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        if sid in done_stats:
            status = f"✓ done ({done_stats[sid]:,} links)"
        elif sid in claimed:
            status = f"⟳ {claimed[sid]}"
        else:
            status = "· pending"
        print(f"  {s['shard_id']}   {s['product_count']:<10} {status}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="THD Distributed Link Collector v2")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto",  action="store_true",
                       help="Claim shards and run continuously until all done")
    group.add_argument("--shard", metavar="NNN",
                       help="Run specific shard (e.g. 001)")
    group.add_argument("--list",  action="store_true",
                       help="Show all shards and status")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--max-shards", type=int, default=999,
                        help="Max shards to run (default: all)")
    args = parser.parse_args()

    if args.list:
        list_shards()
    elif args.auto:
        rate_limiter = RateLimiter()
        done = 0
        while done < args.max_shards:
            shard_id = auto_claim()
            if shard_id is None:
                break
            rate_limiter = run_shard(
                shard_id, dry_run=args.dry_run, rate_limiter=rate_limiter
            ) or rate_limiter
            done += 1
            log.info(f"✅ {done} shards completed. Claiming next...")
        log.info(f"🏁 Finished {done} shards. Total: {rate_limiter.total_requests} req, {rate_limiter.total_206} 206s")
    else:
        run_shard(args.shard.zfill(3), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
