#!/usr/bin/env python3
"""
THD Distributed Image Crawler
==============================
Task coordination via git files — no external API needed.

Claiming a shard:
    Each machine writes tasks/claims/shard_NNN.json and git pushes.
    If two machines race for the same shard, one push wins; the other
    gets a push-rejected error and automatically picks the next shard.

Usage:
    pip install curl_cffi

    python crawler.py --auto       # auto-claim next available shard and run
    python crawler.py --shard 001  # run a specific shard
    python crawler.py --list       # show progress
    python crawler.py --shard 001 --dry-run

Output:
    images/{shard_id}/{category}/{omsid}/*.jpg
    tasks/claims/shard_{id}.json    (claim marker)
    tasks/results/shard_{id}_done.json  (completion marker ✓)
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
IMAGES_DIR  = ROOT / "images"

GRAPHQL_URL = "https://www.homedepot.com/federation-gateway/graphql"
PAGE_DELAY  = 1.5
CDN_DELAY   = 0.2
IMAGE_SIZE  = 1000
MIN_BYTES   = 2000

API_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "x-experience-name": "general-merchandise",
    "Origin": "https://www.homedepot.com",
    "Referer": "https://www.homedepot.com/",
}
IMG_HEADERS = {
    "Accept": "image/avif,image/webp,image/apng,*/*;q=0.8",
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
    """Write claim file, commit, push. Returns True if successful."""
    CLAIMS_DIR.mkdir(parents=True, exist_ok=True)
    claim_path = CLAIMS_DIR / f"shard_{shard_id}.json"
    claim_path.write_text(json.dumps({
        "shard_id": shard_id,
        "machine":  socket.gethostname(),
        "pid":      os.getpid(),
        "claimed_at": datetime.now().isoformat(),
    }, indent=2))

    try:
        git("add", str(claim_path))
        git("commit", "-m", f"claim shard {shard_id} [{socket.gethostname()}]")
        git("push")
        log.info(f"Claimed shard {shard_id}")
        return True
    except RuntimeError as e:
        # Push rejected means someone else claimed it first
        log.info(f"Shard {shard_id} already claimed (push rejected), trying next...")
        # Clean up local commit
        git("reset", "HEAD~1", check=False)
        claim_path.unlink(missing_ok=True)
        git("pull", "--rebase", check=False)
        return False


def git_push_result(shard_id, result_path):
    try:
        git("add", str(result_path))
        git("commit", "-m", f"done shard {shard_id} [{socket.gethostname()}]")
        git("push")
    except RuntimeError as e:
        log.warning(f"Could not push result: {e}")


# ---------------------------------------------------------------------------
# Auto-claim
# ---------------------------------------------------------------------------

def find_unclaimed_shard():
    """Pull latest, return first shard_id with no claim and no result."""
    git_pull()

    manifest   = json.loads((TASKS_DIR / "manifest.json").read_text())
    claimed    = {f.stem for f in CLAIMS_DIR.glob("shard_*.json")} if CLAIMS_DIR.exists() else set()
    done       = {f.stem.replace("_done", "") for f in RESULTS_DIR.glob("shard_*_done.json")} if RESULTS_DIR.exists() else set()
    taken      = claimed | {f"shard_{s}" for s in [r.replace("shard_", "") for r in done]}

    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        if sid not in claimed and f"shard_{s['shard_id']}_done" not in {f.stem for f in RESULTS_DIR.glob("*.json")} if RESULTS_DIR.exists() else True:
            if sid not in claimed:
                return s["shard_id"]
    return None


def auto_claim():
    """Find and atomically claim a shard. Returns shard_id or None."""
    for attempt in range(10):
        shard_id = find_unclaimed_shard()
        if shard_id is None:
            log.info("No unclaimed shards found — all done!")
            return None
        if git_push_claim(shard_id):
            return shard_id
        time.sleep(2)  # brief pause before retrying
    log.error("Could not claim a shard after 10 attempts")
    return None


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def sanitize(s, max_len=50):
    s = re.sub(r'[^\w\-]', '_', str(s))
    return re.sub(r'_+', '_', s).strip('_')[:max_len]


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
            wait = 15 * (attempt + 1)
            log.warning(f"  Rate limited (206), waiting {wait}s")
            time.sleep(wait)
            continue
        if r.status_code != 200:
            return [], None, f"HTTP {r.status_code}"

        data    = r.json()
        product = (data.get("data") or {}).get("product")
        if not product:
            err = (data.get("errors") or [{}])[0].get("message", "no product data")
            return [], None, err

        ident  = product.get("identifiers", {})
        info   = {"brand": ident.get("brandName", ""), "label": ident.get("productLabel", "")}
        images = []
        for img in (product.get("media") or {}).get("images", []):
            url_tmpl = img.get("url", "")
            if not url_tmpl:
                continue
            sizes   = img.get("sizes", [])
            numeric = [s for s in sizes if str(s).isdigit()] if isinstance(sizes, list) else []
            best    = str(max(numeric, key=int)) if numeric else str(IMAGE_SIZE)
            images.append({"url": url_tmpl.replace("<SIZE>", best),
                           "type": img.get("type", ""), "subtype": img.get("subType", "")})
        return images, info, None

    return [], None, "max retries"


def download_image(session, url, filepath):
    try:
        r = session.get(url, headers=IMG_HEADERS, impersonate="chrome", timeout=15)
        if r.status_code == 200 and len(r.content) >= MIN_BYTES:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_bytes(r.content)
            return True
        return False
    except Exception:
        return False


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
    log.info(f"=== Shard {shard_id}/{shard['total_shards']:03d} | {len(products)} products ===")

    if dry_run:
        log.info("[DRY RUN] first 5 products:")
        for p in products[:5]:
            log.info(f"  {p['omsid']} ({p.get('category', '?')})")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    session = cffi_requests.Session()
    stats   = {
        "shard_id": shard_id, "started_at": datetime.now().isoformat(),
        "machine": socket.gethostname(),
        "products_total": len(products), "products_ok": 0,
        "products_failed": 0, "products_no_media": 0,
        "images_downloaded": 0, "images_failed": 0,
    }
    consecutive_errors = 0

    for i, product in enumerate(products):
        omsid = product["omsid"]
        cat   = sanitize(product.get("category", "other"))

        if (i + 1) % 50 == 0:
            eta = (len(products) - i - 1) * PAGE_DELAY / 60
            log.info(f"[{i+1}/{len(products)}] {(i+1)/len(products)*100:.0f}% | ~{eta:.0f}min | {stats['images_downloaded']} imgs")

        images, info, err = graphql_fetch(session, omsid)

        if err:
            consecutive_errors += 1
            stats["products_failed"] += 1
            log.warning(f"  FAIL {omsid}: {err[:80]}")
            if consecutive_errors >= 30:
                log.error("30 consecutive errors — aborting")
                break
            if consecutive_errors >= 5:
                time.sleep(min(60, consecutive_errors * 5))
            time.sleep(PAGE_DELAY)
            continue

        consecutive_errors = 0

        if not images:
            stats["products_no_media"] += 1
            time.sleep(PAGE_DELAY)
            continue

        brand   = sanitize(info.get("brand", "Unknown"))
        out_dir = IMAGES_DIR / shard_id / cat / omsid

        for j, img in enumerate(images):
            fpath = out_dir / f"{brand}_{omsid}_{sanitize(img['type'])}_{j}.jpg"
            if fpath.exists():
                stats["images_downloaded"] += 1
                continue
            if download_image(session, img["url"], fpath):
                stats["images_downloaded"] += 1
            else:
                stats["images_failed"] += 1
            time.sleep(CDN_DELAY)

        stats["products_ok"] += 1
        time.sleep(PAGE_DELAY)

    stats.update({"finished_at": datetime.now().isoformat(), "status": "done"})
    result_file.write_text(json.dumps(stats, indent=2))
    git_push_result(shard_id, result_file)

    log.info("=" * 55)
    log.info(f"SHARD {shard_id} DONE — {stats['images_downloaded']} images downloaded")
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
                d = json.loads(f.read_text())
                claimed[f.stem] = d.get("machine", "?")
            except Exception:
                claimed[f.stem] = "?"

    done = set()
    if RESULTS_DIR.exists():
        done = {f.stem.replace("_done", "") for f in RESULTS_DIR.glob("shard_*_done.json")}

    total   = manifest["n_shards"]
    n_done  = len(done)
    n_claim = len(claimed) - n_done
    print(f"\nProgress: {n_done}/{total} done | {n_claim} in-progress | {total-n_done-n_claim} pending\n")
    print(f"{'Shard':<8} {'Products':<10} {'Status':<25}")
    print("-" * 45)
    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        if sid in done:
            status = "✓ done"
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
    parser = argparse.ArgumentParser(description="THD Distributed Image Crawler")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto",  action="store_true", help="Auto-claim and run next pending shard")
    group.add_argument("--shard", metavar="NNN",       help="Run a specific shard (e.g. 001)")
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
