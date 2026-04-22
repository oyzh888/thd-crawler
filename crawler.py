#!/usr/bin/env python3
"""
THD Distributed Image Crawler
==============================
Each machine runs one shard. Images are saved locally.
When done, a result file is written to tasks/results/.

Usage:
    pip install curl_cffi
    python crawler.py --shard 001          # run shard 001
    python crawler.py --shard 001 --dry-run  # just print what would be done
    python crawler.py --list               # show all shards and their status

Output:
    images/{shard_id}/{category}/{omsid}/  -- downloaded images
    tasks/results/shard_{id}_done.json     -- completion marker (✓)
"""

import argparse
import json
import logging
import re
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
RESULTS_DIR = TASKS_DIR / "results"
IMAGES_DIR  = ROOT / "images"

GRAPHQL_URL = "https://www.homedepot.com/federation-gateway/graphql"
PAGE_DELAY  = 1.5   # seconds between GraphQL calls
CDN_DELAY   = 0.2   # seconds between image downloads
IMAGE_SIZE  = 1000  # max resolution
MIN_BYTES   = 2000  # discard images smaller than this

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


def sanitize(s, max_len=50):
    s = re.sub(r'[^\w\-]', '_', str(s))
    s = re.sub(r'_+', '_', s).strip('_')
    return s[:max_len]


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
                headers=API_HEADERS,
                json=payload,
                impersonate="chrome",
                timeout=20,
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

        data = r.json()
        product = (data.get("data") or {}).get("product")
        if not product:
            err = ""
            if "errors" in data:
                err = data["errors"][0].get("message", "")
            return [], None, err or "no product data"

        ident = product.get("identifiers", {})
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
            best = str(IMAGE_SIZE)
            if isinstance(sizes, list):
                numeric = [s for s in sizes if str(s).isdigit()]
                if numeric:
                    best = str(max(numeric, key=int))
            url = url_tmpl.replace("<SIZE>", best)
            images.append({
                "url": url,
                "type": img.get("type", ""),
                "subtype": img.get("subType", ""),
            })

        return images, info, None

    return [], None, "max retries exceeded"


def download_image(session, url, filepath):
    try:
        r = session.get(url, headers=IMG_HEADERS, impersonate="chrome", timeout=15)
        if r.status_code == 200 and len(r.content) >= MIN_BYTES:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_bytes(r.content)
            return True, len(r.content)
        return False, f"HTTP {r.status_code}" if r.status_code != 200 else f"too small ({len(r.content)}B)"
    except Exception as e:
        return False, str(e)


def run_shard(shard_id, dry_run=False):
    shard_file = SHARDS_DIR / f"shard_{shard_id}.json"
    if not shard_file.exists():
        log.error(f"Shard file not found: {shard_file}")
        sys.exit(1)

    result_file = RESULTS_DIR / f"shard_{shard_id}_done.json"
    if result_file.exists():
        log.info(f"Shard {shard_id} already completed. Delete {result_file} to re-run.")
        return

    shard = json.loads(shard_file.read_text())
    products = shard["products"]
    total_shards = shard["total_shards"]

    log.info(f"=== Shard {shard_id}/{total_shards:03d} | {len(products)} products ===")
    log.info(f"Output: {IMAGES_DIR / shard_id}")
    if dry_run:
        log.info("[DRY RUN] No requests will be made")
        for p in products[:5]:
            log.info(f"  Would scrape: {p['omsid']} ({p['category']})")
        log.info(f"  ... and {len(products)-5} more")
        return

    session = cffi_requests.Session()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    stats = {
        "shard_id": shard_id,
        "started_at": datetime.now().isoformat(),
        "products_total": len(products),
        "products_ok": 0,
        "products_failed": 0,
        "products_no_media": 0,
        "images_downloaded": 0,
        "images_failed": 0,
        "errors": [],
    }

    consecutive_errors = 0

    for i, product in enumerate(products):
        omsid = product["omsid"]
        cat   = sanitize(product.get("category", "other"))

        if (i + 1) % 50 == 0:
            pct = (i + 1) / len(products) * 100
            remaining_h = (len(products) - i - 1) * PAGE_DELAY / 3600
            log.info(f"[{i+1}/{len(products)}] {pct:.0f}% | ~{remaining_h:.1f}h left | {stats['images_downloaded']} imgs")

        images, info, err = graphql_fetch(session, omsid)

        if err:
            consecutive_errors += 1
            stats["products_failed"] += 1
            stats["errors"].append({"omsid": omsid, "error": err})
            log.warning(f"  [{i+1}] FAIL {omsid}: {err[:80]}")
            if consecutive_errors >= 30:
                log.error("30 consecutive errors — aborting shard")
                break
            if consecutive_errors >= 5:
                backoff = min(60, consecutive_errors * 5)
                time.sleep(backoff)
            time.sleep(PAGE_DELAY)
            continue

        consecutive_errors = 0

        if not images:
            stats["products_no_media"] += 1
            time.sleep(PAGE_DELAY)
            continue

        # Download images
        brand = sanitize(info.get("brand", "Unknown"))
        out_dir = IMAGES_DIR / shard_id / cat / omsid
        dl_ok = 0

        for j, img in enumerate(images):
            img_type = sanitize(img["type"])
            fname = f"{brand}_{omsid}_{img_type}_{j}.jpg"
            fpath = out_dir / fname

            if fpath.exists():
                dl_ok += 1
                continue

            ok, detail = download_image(session, img["url"], fpath)
            if ok:
                dl_ok += 1
                stats["images_downloaded"] += 1
            else:
                stats["images_failed"] += 1
            time.sleep(CDN_DELAY)

        stats["products_ok"] += 1
        log.debug(f"  [{i+1}] {omsid} -> {dl_ok}/{len(images)} imgs [{info.get('brand','')}]")

        time.sleep(PAGE_DELAY)

    stats["finished_at"] = datetime.now().isoformat()
    stats["status"] = "done"
    result_file.write_text(json.dumps(stats, indent=2))

    log.info("=" * 55)
    log.info(f"SHARD {shard_id} COMPLETE")
    log.info(f"  Products ok:        {stats['products_ok']}")
    log.info(f"  Products failed:    {stats['products_failed']}")
    log.info(f"  Products no media:  {stats['products_no_media']}")
    log.info(f"  Images downloaded:  {stats['images_downloaded']}")
    log.info(f"  Result file:        {result_file}")
    log.info("=" * 55)


def list_shards():
    manifest = json.loads((TASKS_DIR / "manifest.json").read_text())
    done_files = {f.stem.replace("_done", "") for f in RESULTS_DIR.glob("shard_*_done.json")} if RESULTS_DIR.exists() else set()

    total = manifest["n_shards"]
    done  = len(done_files)
    print(f"\nProgress: {done}/{total} shards done ({done/total*100:.1f}%)\n")
    print(f"{'Shard':<8} {'Products':<10} {'Status'}")
    print("-" * 35)
    for s in manifest["shards"]:
        sid = f"shard_{s['shard_id']}"
        status = "✓ done" if sid in done_files else "· pending"
        print(f"  {s['shard_id']}   {s['product_count']:<10} {status}")
    print()


def main():
    parser = argparse.ArgumentParser(description="THD Distributed Image Crawler")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--shard", metavar="NNN", help="Shard ID to process (e.g. 001)")
    group.add_argument("--list",  action="store_true", help="List all shards and their status")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without making requests")
    args = parser.parse_args()

    if args.list:
        list_shards()
    else:
        shard_id = args.shard.zfill(3)
        run_shard(shard_id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
