# THD Distributed Link Collector

Collects image URLs + product metadata from ~38,500 Home Depot products, split into **78 shards** (~500 products each). **No images are downloaded** — only links + relationships.

## v3: Batch GraphQL (50x fewer requests)

Uses **GraphQL aliases** to fetch 50 products per request:
```graphql
query { p0: product(itemId:"X") {...} p1: product(itemId:"Y") {...} ... }
```

| Metric | v2 (old) | v3 (current) |
|--------|----------|-------------|
| Requests per shard | 500 | **10** |
| Total requests (all 78 shards) | 38,545 | **~771** |
| One residential IP can do | ~1 shard | **all 78 shards** |
| Time per shard | ~15 min | **~30 sec** |

Each machine claims one shard via git, runs it, pushes results back.

## Quick Start (each machine)

```bash
git clone https://github.com/oyzh888/thd-crawler.git
cd thd-crawler
pip install curl_cffi

# Auto-claim next available shard and run:
python crawler.py --auto
```

The script will:
1. `git pull` to see what's already claimed
2. Write `tasks/claims/shard_NNN.json` and push (atomic claim via git)
3. If two machines race, one push wins — the other auto-retries next shard
4. Fetch 50 products per GraphQL request using aliases (10 requests/shard)
5. Collect image URLs + metadata to `tasks/links/shard_NNN_links.jsonl`
6. Write `tasks/results/shard_NNN_done.json` and push when finished

## Output Format

`tasks/links/shard_NNN_links.jsonl` — one line per image URL:

```json
{
  "omsid":      "314427520",
  "page_url":   "https://www.homedepot.com/p/.../314427520",
  "category":   "Cordless Circular Saw",
  "brand":      "DEWALT",
  "model":      "DCS565B",
  "label":      "20V MAX Cordless 6.5 in. Circular Saw",
  "img_url":    "https://images.thdstatic.com/productImages/...jpg",
  "img_type":   "IMAGE",
  "img_subtype": "PRIMARY"
}
```

## Resume Support (auto)

If your machine gets rate-limited or killed mid-shard, **just re-run the same shard**. The script will:

1. Read your existing `shard_NNN_links.jsonl`
2. Detect which `omsid`s are already collected
3. Skip them and continue from where you left off

```bash
python crawler.py --shard 007    # resumes automatically if partial data exists
```

Each product's links are `fsync`'d to disk immediately, so even `kill -9` won't lose more than the in-flight request.

## Check Progress

```bash
python crawler.py --list
```

```
Progress: 12/78 done | 3 running | 63 pending | 42,135 links collected

Shard    Products   Status
---------------------------------------------
  001    500        ✓ done (3,521 links)
  002    500        ✓ done (4,018 links)
  003    500        ⟳ MacBook-Pro-Minhou
  004    500        · pending
  ...
```

## Stats

| | |
|---|---|
| Total products | 38,545 |
| Shards | 78 × 500 products |
| Est. total links | ~300,000 |
| Batch size | 50 products/request |
| Requests per shard | ~10 |
| Total requests (all shards) | ~771 |
| Est. time per shard | ~30 sec |
| One residential IP | can finish everything (~40 min) |

## Manual shard / dry run

```bash
python crawler.py --shard 007          # run specific shard
python crawler.py --shard 007 --dry-run  # preview without requests
```

## ⚠️ Rate Limit Rules (IMPORTANT)

HD GraphQL endpoint (`homedepot.com/federation-gateway/graphql`) has IP-level rate limiting:

| Rule | Detail |
|------|--------|
| **Safe throughput** | 1 instance per IP, PAGE_DELAY ≥ 2s |
| **Datacenter IP limit** | ~200 requests then HTTP 206 |
| **Residential IP limit** | ~500 requests/day (rough estimate) |
| **Parallel from same IP** | ❌ 3+ instances → 206 within 30 seconds |
| **Recovery time** | 10+ minutes after being blocked (possibly longer) |
| **CDN (images.thdstatic.com)** | No rate limit, unlimited parallel |

### What 206 looks like
```json
{"data":{"GenericError":null},"error":[{"message":"Generic errors"}]}
```

The script auto-detects 206 and:
- Prints clear warning: `⚠️  IP RATE LIMITED — HD limits ~500 req/IP/day`
- Increases delay dynamically (2s → 10s max)
- Aborts after 30 consecutive failures with a "switch IP" hint

### Best Practices

1. **1 crawler per IP** — never run 2+ instances on the same machine
2. **Residential IP preferred** — datacenter IPs get blocked after ~200 requests
3. **If you see 206** — stop, wait 10+ min or switch IP/VPN, then re-run (resume kicks in automatically)
4. **Multiple machines** — use different IPs (different machines/VPNs), 1 instance each

### Estimated throughput

| Setup | Speed | Full run (78 shards) |
|-------|-------|---------------------|
| 1 residential IP | ~15 min/shard | ~20 hours |
| 1 datacenter IP | ~200 products then blocked | partial shard + needs IP swap |
| 5 residential IPs | parallel | ~4 hours |
| 10 residential IPs | parallel | ~2 hours |
