# THD Distributed Image Crawler

Scrapes ~38,500 Home Depot products across 78 shards. Each machine takes one shard (~500 products, ~15 min).

## Quick Start

```bash
# 1. Install
pip install curl_cffi

# 2. Pick any unclaimed shard (check list first)
python crawler.py --list

# 3. Run it
python crawler.py --shard 001
```

Images saved to `images/NNN/{category}/{omsid}/*.jpg`
Done marker written to `tasks/results/shard_NNN_done.json`

## Check Overall Progress

```bash
python crawler.py --list
```

```
Progress: 12/78 shards done (15.4%)

Shard    Products   Status
-----------------------------------
  001    500        ✓ done
  002    500        ✓ done
  003    500        · pending
  ...
```

## Stats

| Item | Count |
|------|-------|
| Total products | 38,545 |
| Shards | 78 |
| Products per shard | ~500 |
| Est. images per product | ~8–10 |
| **Est. total images** | **~350,000** |
| Est. time per shard | ~15 min |
| Est. time (78 machines parallel) | ~15 min total |

## Coordinate with Team

- Check `--list` to see which shards are pending
- Claim a shard number with your team (e.g. via Slack)
- Run it, result auto-saved when done
- Push `tasks/results/shard_NNN_done.json` back to repo so others can see progress

## Notes

- Supports **resume**: already-downloaded images are skipped, re-run a shard safely
- Rate limiting: uses `curl_cffi` Chrome impersonation + 1.5s delay between products
- If you hit HTTP 206 (rate limit), the script backs off automatically
- Images are 1000px max resolution JPEG
