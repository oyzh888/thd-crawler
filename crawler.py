#!/usr/bin/env python3
"""
THD Distributed Link Collector — v3 Batch GraphQL
===================================================
Key improvement: uses GraphQL aliases to fetch 50 products per request.
  - 500 products/shard ÷ 50/batch = only 10 requests per shard
  - Rate limit at ~200 req/IP → 1 IP can do 200×50 = 10,000 products
  - One MacBook can finish all 78 shards with ~770 total requests

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

# --- Batch settings ---
BATCH_SIZE     = 50     # products per GraphQL request (via aliases)
BURST_LIMIT    = 180    # requests before proactive pause
BURST_DELAY    = 3.0    # seconds between batch requests
COOLDOWN_PROBE = 120    # seconds between probe attempts during cooldown
COOLDOWN_MAX   = 1800   # max cooldown (30 min)
JITTER         = 0.5    # random jitter on delays

API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-experience-name": "general-merchandise",
    "Origin": "https://www.homedepot.com",
    "Referer": "https://www.homedepot.com/",
}

# Single-product query for probing rate limit recovery
PROBE_QUERY = """query productClientOnlyProduct($itemId: String!) {
  product(itemId: $itemId) { itemId }
}"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self):
        self.requests_in_burst = 0
        self.total_requests = 0
        self.total_products = 0  # track products, not just requests
        self.total_206 = 0
        self.cooldowns = 0

    def pre_request(self, batch_size=1):
        if self.requests_in_burst >= BURST_LIMIT:
            self._proactive_cooldown()

    def post_request(self, status_code, products_in_batch=1):
        self.total_requests += 1
        if status_code == 200:
            self.requests_in_burst += 1
            self.total_products += products_in_batch
            return True
        elif status_code == 206:
            self.total_206 += 1
            self._reactive_cooldown()
            return False
        return False

    def _proactive_cooldown(self):
        pause = random.uniform(600, 900)
        self.cooldowns += 1
        log.info(
            f"  Proactive pause after {self.requests_in_burst} requests "
            f"({self.total_products} products). Sleeping {pause/60:.0f} min..."
        )
        time.sleep(pause)
        self.requests_in_burst = 0

    def _reactive_cooldown(self):
        log.warning(
            f"  Rate limited (206) at {self.requests_in_burst} requests. "
            f"Probing every {COOLDOWN_PROBE}s..."
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
                        "query": PROBE_QUERY,
                    },
                    impersonate="chrome", timeout=10,
                )
                if resp.status_code == 200:
                    log.info(f"  Recovered after {waited/60:.0f} min!")
                    return
                log.info(f"  Still 206... ({waited/60:.0f} min)")
            except Exception:
                log.info(f"  Probe failed... ({waited/60:.0f} min)")

        log.warning(f"  Not recovered after {COOLDOWN_MAX/60:.0f} min. Continuing anyway.")

    def stats_dict(self):
        return {
            "total_requests": self.total_requests,
            "total_products_fetched": self.total_products,
            "total_206": self.total_206,
            "cooldowns": self.cooldowns,
        }


# ---------------------------------------------------------------------------
# Batch GraphQL — the key optimization
# ---------------------------------------------------------------------------

def build_batch_query(product_ids):
    """Build one GraphQL query that fetches N products via aliases.

    Example output (N=3):
        {
          p0: product(itemId: "203164241") { itemId identifiers { ... } media { ... } }
          p1: product(itemId: "314427520") { itemId identifiers { ... } media { ... } }
          p2: product(itemId: "325544370") { itemId identifiers { ... } media { ... } }
        }

    This counts as 1 HTTP request against rate limit, but returns N products.
    """
    parts = []
    for i, pid in enumerate(product_ids):
        parts.append(
            f'p{i}: product(itemId: "{pid}") {{ '
            f'itemId '
            f'identifiers {{ productLabel brandName modelNumber }} '
            f'media {{ images {{ url type subType sizes }} }} '
            f'}}'
        )
    return "{ " + " ".join(parts) + " }"


def parse_batch_response(data, product_ids):
    """Parse batch response into per-product results.

    Returns: list of (omsid, images_list, info_dict, error_or_None)
    """
    results = []
    gql_data = data.get("data", {})

    for i, pid in enumerate(product_ids):
        product = gql_data.get(f"p{i}")
        if not product:
            results.append((pid, [], None, "no data"))
            continue

        ident = product.get("identifiers") or {}
        info = {
            "brand": ident.get("brandName", ""),
            "model": ident.get("modelNumber", ""),
            "label": ident.get("productLabel", ""),
        }

        images = []
        for img in (product.get("media") or {}).get("images", []):
            url_tmpl = img.get("url", "")
            if not url_tmpl:
                continue
            sizes = img.get("sizes", [])
            numeric = [s for s in sizes if str(s).isdigit()] if isinstance(sizes, list) else []
            best = str(max(numeric, key=int)) if numeric else str(IMAGE_SIZE)
            images.append({
                "img_url":     url_tmpl.replace("<SIZE>", best),
                "img_type":    img.get("type", ""),
                "img_subtype": img.get("subType", ""),
            })

        results.append((pid, images, info, None))

    return results


def graphql_batch_fetch(session, product_ids, rate_limiter):
    """Fetch N products in one GraphQL request using aliases.

    Returns: list of (omsid, images, info, error)
    """
    query = build_batch_query(product_ids)
    rate_limiter.pre_request(len(product_ids))

    for attempt in range(3):
        try:
            r = session.post(
                f"{GRAPHQL_URL}?opname=batchProducts",
                headers=API_HEADERS,
                json={"query": query},
                impersonate="chrome",
                timeout=30,
            )
        except Exception as e:
            return [(pid, [], None, str(e)) for pid in product_ids]

        if r.status_code == 206:
            rate_limiter.post_request(206, 0)
            continue

        rate_limiter.post_request(r.status_code, len(product_ids))

        if r.status_code != 200:
            return [(pid, [], None, f"HTTP {r.status_code}") for pid in product_ids]

        try:
            data = r.json()
        except Exception:
            return [(pid, [], None, "JSON parse error") for pid in product_ids]

        if "errors" in data and not data.get("data"):
            err = data["errors"][0].get("message", "unknown error") if data["errors"] else "unknown"
            return [(pid, [], None, err) for pid in product_ids]

        return parse_batch_response(data, product_ids)

    return [(pid, [], None, "max retries") for pid in product_ids]


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
            log.info("No unclaimed shards found — all done!")
            return None
        if git_push_claim(shard_id):
            return shard_id
        time.sleep(2)
    log.error("Could not claim a shard after 10 attempts")
    return None


# ---------------------------------------------------------------------------
# Shard runner (batch mode)
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
    n_batches = (len(products) + BATCH_SIZE - 1) // BATCH_SIZE

    log.info(f"=== Shard {shard_id}/{shard['total_shards']:03d} | "
             f"{len(products)} products | {n_batches} batches of {BATCH_SIZE} ===")

    if dry_run:
        log.info(f"[DRY RUN] {n_batches} GraphQL requests needed (vs {len(products)} in v2)")
        for p in products[:5]:
            log.info(f"  {p['omsid']} ({p.get('category','?')})")
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
            log.info(f"Resume: {len(processed)} products done ({existing_links} links).")

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
        "batch_size":        BATCH_SIZE,
    }

    # Build product lookup for metadata
    product_meta = {p["omsid"]: p for p in products}

    # Filter out already-processed
    remaining = [p for p in products if p["omsid"] not in processed]
    if not remaining:
        log.info(f"All {len(products)} products already processed.")
    else:
        log.info(f"{len(remaining)} products remaining → {(len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE} requests")

    # Process in batches
    with links_file.open("a", encoding="utf-8") as f:
        for batch_idx in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[batch_idx:batch_idx + BATCH_SIZE]
            batch_ids = [p["omsid"] for p in batch]
            batch_num = batch_idx // BATCH_SIZE + 1
            total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE

            log.info(
                f"  Batch {batch_num}/{total_batches} "
                f"({len(batch_ids)} products) | "
                f"req={rate_limiter.total_requests} "
                f"links={stats['links_collected']} "
                f"burst={rate_limiter.requests_in_burst}/{BURST_LIMIT}"
            )

            results = graphql_batch_fetch(session, batch_ids, rate_limiter)

            for omsid, images, info, err in results:
                meta = product_meta.get(omsid, {})
                page_url = meta.get("url", f"https://www.homedepot.com/p/{omsid}")
                category = meta.get("category", "other")

                if err:
                    stats["products_failed"] += 1
                    continue

                if not images:
                    stats["products_no_media"] += 1
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

                stats["products_ok"] += 1

            f.flush()
            os.fsync(f.fileno())

            # Delay between batches
            delay = BURST_DELAY + random.uniform(-JITTER, JITTER)
            time.sleep(max(1.0, delay))

    stats.update({
        "finished_at": datetime.now().isoformat(),
        "status": "done",
        **rate_limiter.stats_dict(),
    })
    result_file.write_text(json.dumps(stats, indent=2))
    git_push_result(shard_id, [result_file, links_file])

    log.info("=" * 55)
    log.info(
        f"SHARD {shard_id} DONE — {stats['links_collected']} links "
        f"in {rate_limiter.total_requests} requests"
    )
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

    print(f"\nProgress: {n_done}/{total} done | {n_run} running | "
          f"{total-n_done-n_run} pending | {total_links:,} links collected\n")
    print(f"{'Shard':<8} {'Products':<10} {'Status':<30}")
    print("-" * 50)
    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        if sid in done_stats:
            status = f"done ({done_stats[sid]:,} links)"
        elif sid in claimed:
            status = f"running [{claimed[sid][:25]}]"
        else:
            status = "pending"
        print(f"  {s['shard_id']}   {s['product_count']:<10} {status}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global BATCH_SIZE
    parser = argparse.ArgumentParser(
        description="THD Link Collector v3 — batch GraphQL (50 products/request)")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto",  action="store_true",
                       help="Claim shards and run continuously")
    group.add_argument("--shard", metavar="NNN",
                       help="Run specific shard (e.g. 001)")
    group.add_argument("--list",  action="store_true",
                       help="Show all shards and status")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--max-shards", type=int, default=999,
                        help="Max shards to run (default: all)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Products per GraphQL request (default: {BATCH_SIZE})")
    args = parser.parse_args()

    BATCH_SIZE = args.batch_size

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
            log.info(f"  {done} shards done. Claiming next...")
        log.info(
            f"Finished {done} shards. "
            f"{rate_limiter.total_requests} requests, "
            f"{rate_limiter.total_products} products, "
            f"{rate_limiter.total_206} rate limits"
        )
    else:
        run_shard(args.shard.zfill(3), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
