#!/usr/bin/env python3
"""
THD Distributed Link Collector
================================
Collects image URLs + product metadata via GraphQL. No image downloads.

Each machine auto-claims a shard via git, collects links, pushes results back.

Output per shard:
    tasks/results/shard_{id}_done.json   -- completion marker + stats
    tasks/links/shard_{id}_links.jsonl   -- one line per image URL

Each JSONL line:
    {
        "omsid":      "314427520",
        "page_url":   "https://www.homedepot.com/p/.../314427520",
        "category":   "Cordless Circular Saw",
        "brand":      "DEWALT",
        "model":      "DCS565B",
        "label":      "20V MAX Cordless 6.5 in. Circular Saw",
        "img_url":    "https://images.thdstatic.com/...",
        "img_type":   "IMAGE",
        "img_subtype": "PRIMARY"
    }

Usage:
    pip install curl_cffi

    python crawler.py --auto       # auto-claim next shard and run
    python crawler.py --shard 001  # run specific shard
    python crawler.py --list       # show progress
    python crawler.py --shard 001 --dry-run
"""

import argparse
import json
import logging
import os
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
PAGE_DELAY      = 2.0   # seconds between requests
PAGE_DELAY_MAX  = 10.0  # max delay after repeated rate limits
IMAGE_SIZE      = 1000  # preferred resolution

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

def graphql_fetch(session, omsid):
    payload = {
        "operationName": "productClientOnlyProduct",
        "variables": {"itemId": omsid},
        "query": PRODUCT_MEDIA_QUERY,
    }
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
            wait = 30 * (attempt + 1)
            log.warning(
                f"  ⚠️  IP RATE LIMITED (HTTP 206) — HD limits ~500 req/IP/day. "
                f"Backing off {wait}s (attempt {attempt+1}/3). "
                f"If this persists, switch IP/VPN."
            )
            time.sleep(wait)
            continue
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

def run_shard(shard_id, dry_run=False):
    shard_file  = SHARDS_DIR / f"shard_{shard_id}.json"
    result_file = RESULTS_DIR / f"shard_{shard_id}_done.json"

    if not shard_file.exists():
        log.error(f"Shard file not found: {shard_file}")
        sys.exit(1)
    if result_file.exists():
        log.info(f"Shard {shard_id} already completed.")
        return

    shard    = json.loads(shard_file.read_text())
    products = shard["products"]
    log.info(f"=== Shard {shard_id}/{shard['total_shards']:03d} | {len(products)} products (links only) ===")

    if dry_run:
        log.info("[DRY RUN] first 5 products:")
        for p in products[:5]:
            log.info(f"  {p['omsid']} ({p.get('category','?')}) — {p['url']}")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LINKS_DIR.mkdir(parents=True, exist_ok=True)

    links_file = LINKS_DIR / f"shard_{shard_id}_links.jsonl"

    # --- Resume: read any already-collected omsids from the JSONL file ---
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
            log.info(
                f"Resume detected: {len(processed)} products already collected "
                f"({existing_links} links). Will skip them and continue."
            )

    session = cffi_requests.Session()

    stats = {
        "shard_id":         shard_id,
        "machine":          socket.gethostname(),
        "started_at":       datetime.now().isoformat(),
        "products_total":   len(products),
        "products_ok":      len(processed),
        "products_failed":  0,
        "products_no_media": 0,
        "links_collected":  existing_links,
        "resumed":          len(processed) > 0,
    }

    consecutive_errors = 0
    current_delay = PAGE_DELAY
    rate_limit_hits = 0

    # Open in APPEND mode so resumes preserve existing links
    with links_file.open("a", encoding="utf-8") as f:
        for i, product in enumerate(products):
            omsid    = product["omsid"]
            page_url = product.get("url", f"https://www.homedepot.com/p/{omsid}")
            category = product.get("category", "other")

            # Resume: skip products we've already collected links for
            if omsid in processed:
                continue

            if (i + 1) % 100 == 0:
                eta = (len(products) - i - 1) * current_delay / 60
                log.info(
                    f"[{i+1}/{len(products)}] {(i+1)/len(products)*100:.0f}% | "
                    f"~{eta:.0f}min | {stats['links_collected']} links | "
                    f"delay={current_delay:.1f}s | rate_limits={rate_limit_hits}"
                )

            images, info, err = graphql_fetch(session, omsid)

            if err:
                consecutive_errors += 1
                stats["products_failed"] += 1
                if "206" in str(err) or "max retries" in str(err):
                    rate_limit_hits += 1
                    # Slow down globally when rate limited
                    current_delay = min(PAGE_DELAY_MAX, current_delay * 1.5)
                    log.warning(
                        f"  ⚠️  Rate limit detected — slowing to {current_delay:.1f}s/req. "
                        f"Total rate limit hits: {rate_limit_hits}. "
                        f"Consider switching IP if this keeps happening."
                    )
                else:
                    log.warning(f"  FAIL {omsid}: {err[:80]}")
                if consecutive_errors >= 30:
                    log.error("30 consecutive errors — aborting shard. Switch IP and re-run.")
                    break
                if consecutive_errors >= 5:
                    time.sleep(min(120, consecutive_errors * 10))
                time.sleep(current_delay)
                continue

            consecutive_errors = 0
            # Gradually recover delay after clean run
            if current_delay > PAGE_DELAY and i % 20 == 0:
                current_delay = max(PAGE_DELAY, current_delay * 0.9)

            if not images:
                stats["products_no_media"] += 1
                time.sleep(PAGE_DELAY)
                continue

            for img in images:
                row = {
                    "omsid":       omsid,
                    "page_url":    page_url,
                    "category":    category,
                    "brand":       info.get("brand", ""),
                    "model":       info.get("model", ""),
                    "label":       info.get("label", ""),
                    **img,
                }
                f.write(json.dumps(row) + "\n")
                stats["links_collected"] += 1
            f.flush()  # survive kill -9
            os.fsync(f.fileno())
            processed.add(omsid)
            stats["products_ok"] += 1
            time.sleep(current_delay)

    stats.update({"rate_limit_hits": rate_limit_hits, "finished_at": datetime.now().isoformat(), "status": "done"})
    result_file.write_text(json.dumps(stats, indent=2))
    git_push_result(shard_id, [result_file, links_file])

    log.info("=" * 55)
    log.info(f"SHARD {shard_id} DONE — {stats['links_collected']} links collected")
    log.info(f"  Links file: {links_file}")
    log.info("=" * 55)


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
    parser = argparse.ArgumentParser(description="THD Distributed Link Collector")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto",  action="store_true", help="Auto-claim and run next pending shard")
    group.add_argument("--shard", metavar="NNN",       help="Run specific shard (e.g. 001)")
    group.add_argument("--list",  action="store_true", help="Show all shards and status")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.list:
        list_shards()
    elif args.auto:
        shard_id = auto_claim()
        if shard_id:
            run_shard(shard_id, dry_run=args.dry_run)
    else:
        run_shard(args.shard.zfill(3), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
