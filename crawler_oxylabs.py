#!/usr/bin/env python3
"""
THD Link Collector — Oxylabs Web Unblocker Edition
====================================================
Uses Oxylabs Web Unblocker proxy to bypass Akamai.
Single-product queries (batch aliases don't work through unblocker).
But since there's no rate limit via proxy, we can run many concurrent requests.

Usage:
    pip install requests  # no curl_cffi needed!

    python crawler_oxylabs.py --auto                 # run all pending shards
    python crawler_oxylabs.py --auto --max-shards 5  # run up to 5 shards
    python crawler_oxylabs.py --shard 001            # specific shard
    python crawler_oxylabs.py --list                 # show progress
"""

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests

ROOT        = Path(__file__).resolve().parent
TASKS_DIR   = ROOT / "tasks"
SHARDS_DIR  = TASKS_DIR / "shards"
CLAIMS_DIR  = TASKS_DIR / "claims"
RESULTS_DIR = TASKS_DIR / "results"
LINKS_DIR   = TASKS_DIR / "links"

GRAPHQL_URL = "https://www.homedepot.com/federation-gateway/graphql"
IMAGE_SIZE  = 1000

# --- Oxylabs config ---
PROXY_URL  = "https://unblock.oxylabs.io:60000"
PROXY_USER = "oyzh888_QmK6o"
PROXY_PASS = "__Oyzh655890"

# --- Concurrency ---
CONCURRENCY    = 10      # parallel requests through proxy
REQUEST_DELAY  = 0.3     # delay between launching requests
MAX_RETRIES    = 3

GRAPHQL_QUERY = """query productClientOnlyProduct($itemId: String!) {
  product(itemId: $itemId) {
    itemId
    identifiers { productLabel brandName modelNumber }
    media { images { url type subType sizes } }
  }
}"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler-oxylabs")


# ---------------------------------------------------------------------------
# GraphQL fetch via Oxylabs
# ---------------------------------------------------------------------------

def fetch_product(omsid: str) -> tuple:
    """Fetch one product via Oxylabs proxy.
    Returns: (omsid, images_list, info_dict, error_or_None)
    """
    proxies = {
        "https": f"https://{PROXY_USER}:{PROXY_PASS}@unblock.oxylabs.io:60000",
    }

    payload = {
        "operationName": "productClientOnlyProduct",
        "variables": {"itemId": str(omsid)},
        "query": GRAPHQL_QUERY,
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-experience-name": "general-merchandise",
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                f"{GRAPHQL_URL}?opname=productClientOnlyProduct",
                headers=headers,
                json=payload,
                proxies=proxies,
                timeout=30,
                verify=False,  # Oxylabs uses self-signed cert
            )
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
                continue
            return (omsid, [], None, f"request error: {e}")

        if r.status_code == 206:
            # Rate limited even through proxy — wait and retry
            time.sleep(5 * (attempt + 1))
            continue

        if r.status_code != 200:
            return (omsid, [], None, f"HTTP {r.status_code}")

        try:
            data = r.json()
        except Exception:
            return (omsid, [], None, "JSON parse error")

        product = (data.get("data") or {}).get("product")
        if not product:
            err_msg = ""
            if "errors" in data:
                err_msg = data["errors"][0].get("message", "") if data["errors"] else ""
            return (omsid, [], None, err_msg or "no product data")

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

        return (omsid, images, info, None)

    return (omsid, [], None, "max retries (206)")


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def git(*args, check=True):
    r = subprocess.run(
        ["git", "-C", str(ROOT)] + list(args),
        capture_output=True, text=True, timeout=60,
    )
    if check and r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r

def git_pull():
    try:
        git("pull", "--rebase", "--autostash")
    except Exception as e:
        log.warning(f"git pull failed: {e}")

def git_push_claim(shard_id):
    claim_file = CLAIMS_DIR / f"shard_{shard_id}.json"
    CLAIMS_DIR.mkdir(parents=True, exist_ok=True)
    claim_file.write_text(json.dumps({
        "shard_id": shard_id,
        "machine": socket.gethostname() + "-oxylabs",
        "pid": os.getpid(),
        "claimed_at": datetime.now().isoformat(),
    }, indent=2))
    try:
        git("add", str(claim_file.relative_to(ROOT)))
        git("commit", "-m", f"claim shard {shard_id} (oxylabs)")
        git("push")
        log.info(f"✅ Claimed shard {shard_id}")
        return True
    except Exception as e:
        log.warning(f"Push claim failed (race?): {e}")
        git("reset", "HEAD~1", check=False)
        git("checkout", "--", ".", check=False)
        git_pull()
        return False

def git_push_result(shard_id, files):
    try:
        for f in files:
            git("add", str(Path(f).relative_to(ROOT)))
        git("commit", "-m", f"shard {shard_id} done (oxylabs)")
        git("push")
        log.info(f"📤 Pushed results for shard {shard_id}")
    except Exception as e:
        log.warning(f"Push results failed: {e}")
        for retry in range(3):
            try:
                git_pull()
                git("push")
                log.info(f"📤 Pushed on retry {retry+1}")
                return
            except Exception:
                time.sleep(2)

def find_unclaimed_shard():
    manifest = json.loads((TASKS_DIR / "manifest.json").read_text())
    claimed = set()
    if CLAIMS_DIR.exists():
        for f in CLAIMS_DIR.glob("shard_*.json"):
            claimed.add(f.stem.replace("shard_", ""))
    done = set()
    if RESULTS_DIR.exists():
        for f in RESULTS_DIR.glob("shard_*_done.json"):
            try:
                d = json.loads(f.read_text())
                done.add(d["shard_id"])
            except Exception:
                pass

    for s in manifest["shards"]:
        sid = s["shard_id"]
        if sid not in claimed and sid not in done:
            return sid
    return None

def claim_shard():
    git_pull()
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
# Shard runner (concurrent via Oxylabs)
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

    log.info(f"=== Shard {shard_id}/{shard['total_shards']:03d} | "
             f"{len(products)} products | concurrency={CONCURRENCY} ===")

    if dry_run:
        log.info(f"[DRY RUN] Would fetch {len(products)} products via Oxylabs")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LINKS_DIR.mkdir(parents=True, exist_ok=True)

    links_file = LINKS_DIR / f"shard_{shard_id}_links.jsonl"

    # --- Resume ---
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

    product_meta = {p["omsid"]: p for p in products}
    remaining = [p for p in products if p["omsid"] not in processed]

    if not remaining:
        log.info(f"All {len(products)} products already processed.")
    else:
        log.info(f"{len(remaining)} products to fetch via Oxylabs proxy")

    stats = {
        "shard_id":          shard_id,
        "machine":           socket.gethostname() + "-oxylabs",
        "started_at":        datetime.now().isoformat(),
        "products_total":    len(products),
        "products_ok":       len(processed),
        "products_failed":   0,
        "products_no_media": 0,
        "links_collected":   existing_links,
        "total_requests":    0,
        "total_206":         0,
    }

    t0 = time.time()

    with links_file.open("a", encoding="utf-8") as f:
        # Process in concurrent batches
        for chunk_start in range(0, len(remaining), CONCURRENCY * 10):
            chunk = remaining[chunk_start:chunk_start + CONCURRENCY * 10]

            with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                futures = {}
                for i, p in enumerate(chunk):
                    # Stagger requests slightly
                    if i > 0 and i % CONCURRENCY == 0:
                        time.sleep(REQUEST_DELAY)
                    future = executor.submit(fetch_product, p["omsid"])
                    futures[future] = p

                for future in as_completed(futures):
                    p = futures[future]
                    omsid, images, info, err = future.result()
                    meta = product_meta.get(omsid, {})
                    page_url = meta.get("url", f"https://www.homedepot.com/p/{omsid}")
                    category = meta.get("category", "other")
                    stats["total_requests"] += 1

                    if err:
                        if "206" in str(err):
                            stats["total_206"] += 1
                        stats["products_failed"] += 1
                        continue

                    if not images:
                        stats["products_no_media"] += 1
                        stats["products_ok"] += 1
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

            elapsed = time.time() - t0
            rate = stats["products_ok"] / max(elapsed, 1) * 60
            log.info(
                f"  Progress: {stats['products_ok']}/{len(products)} products | "
                f"{stats['links_collected']} links | "
                f"{stats['products_failed']} failed | "
                f"{rate:.0f} products/min | "
                f"{elapsed:.0f}s elapsed"
            )

    elapsed = time.time() - t0
    stats.update({
        "finished_at": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "status": "done",
    })
    result_file.write_text(json.dumps(stats, indent=2))
    git_push_result(shard_id, [result_file, links_file])

    log.info("=" * 55)
    log.info(
        f"SHARD {shard_id} DONE — {stats['links_collected']} links "
        f"from {stats['products_ok']} products in {elapsed:.0f}s "
        f"({stats['products_failed']} failed, {stats['total_206']} rate-limited)"
    )
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
    global CONCURRENCY
    # Suppress InsecureRequestWarning from urllib3
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(
        description="THD Link Collector — Oxylabs Web Unblocker Edition")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auto",  action="store_true")
    group.add_argument("--shard", metavar="NNN")
    group.add_argument("--list",  action="store_true")
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--max-shards",  type=int, default=999)
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Parallel requests (default: {CONCURRENCY})")
    args = parser.parse_args()
    CONCURRENCY = args.concurrency

    if args.list:
        list_shards()
        return

    if args.shard:
        run_shard(args.shard.zfill(3), dry_run=args.dry_run)
        return

    # --auto mode
    shards_done = 0
    while shards_done < args.max_shards:
        shard_id = claim_shard()
        if shard_id is None:
            break
        run_shard(shard_id, dry_run=args.dry_run)
        shards_done += 1
        log.info(f"Completed {shards_done}/{args.max_shards} shards")

    log.info(f"Finished — {shards_done} shards processed")


if __name__ == "__main__":
    main()
