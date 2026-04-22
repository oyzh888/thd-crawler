# THD Distributed Image Crawler

Scrapes ~38,500 Home Depot products split into **78 shards** (~500 products each).
Each machine claims one shard via git, runs it, pushes results back.

## Quick Start (each machine)

```bash
git clone https://github.com/oyzh888/thd-crawler.git
cd thd-crawler
pip install curl_cffi

# Auto-claim next available shard and run:
python crawler.py --auto
```

That's it. The script will:
1. `git pull` to see what's already claimed
2. Write `tasks/claims/shard_NNN.json` and push (atomic claim)
3. If two machines race for the same shard, one push wins — the other retries automatically
4. Download all images to `images/NNN/{category}/{omsid}/`
5. Write `tasks/results/shard_NNN_done.json` and push when finished

## Check Progress

```bash
python crawler.py --list
```

```
Progress: 12/78 done | 3 in-progress | 63 pending

Shard    Products   Status
---------------------------------------------
  001    500        ✓ done
  002    500        ✓ done
  003    500        ⟳ MacBook-Pro-Minhou    ← someone is running this
  004    500        · pending
  ...
```

## Stats

| | |
|---|---|
| Total products | 38,545 |
| Shards | 78 × 500 products |
| Est. images | ~350,000 |
| Est. time per shard | ~15 min |
| With 10 machines | ~2 hours total |

## Manual shard

```bash
python crawler.py --shard 007          # run shard 007 specifically
python crawler.py --shard 007 --dry-run  # preview without requests
```

## ⚠️ Rate Limit Rules (IMPORTANT)

HD GraphQL endpoint (`homedepot.com/federation-gateway/graphql`) has IP-level rate limiting:

| Rule | Detail |
|------|--------|
| **Safe throughput** | 1 instance per IP, PAGE_DELAY ≥ 1.5s |
| **Datacenter IP limit** | ~200 requests then HTTP 206 |
| **Residential IP limit** | No limit observed (ran 500+ requests) |
| **Parallel from same IP** | ❌ 3+ instances → 206 within 30 seconds |
| **Recovery time** | 10+ minutes after being blocked (possibly longer) |
| **CDN (images.thdstatic.com)** | No rate limit, unlimited parallel |

### 206 Response (rate limited)
```json
{"data":{"GenericError":null},"error":[{"message":"Generic errors"}]}
```

### Best Practices

1. **1 crawler per IP** — never run 2+ instances on the same machine
2. **Residential IP preferred** — datacenter IPs get blocked after ~200 requests
3. **If you get 206** — stop immediately, wait 10+ minutes, then resume
4. **Multiple machines** — use different IPs (different machines/VPNs), 1 instance each
5. **Don't over-claim** — each shard is 500 products; datacenter IPs may not finish a full shard

### Estimated throughput

| Setup | Speed | Full run (78 shards) |
|-------|-------|---------------------|
| 1 residential IP | ~15 min/shard | ~20 hours |
| 1 datacenter IP | ~200 products then blocked | can't finish 1 shard |
| 5 residential IPs | ~15 min/shard × parallel | ~4 hours |
| 10 residential IPs | ~15 min/shard × parallel | ~2 hours |
