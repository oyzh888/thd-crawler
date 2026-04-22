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
