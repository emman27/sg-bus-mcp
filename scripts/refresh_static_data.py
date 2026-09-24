#!/usr/bin/env python3
"""Rebuild the bundled bus datasets in data/ from LTA DataMall.

Pages through the server's dump_static_data maintenance tool (via the
sg-bus CLI, which attaches the stored connector credential) and writes:

    data/bus_stops.json    — raw /BusStops records
    data/bus_routes.json   — raw /BusRoutes records

Run this whenever LTA's static data changes (new services, new stops —
a few times a year is plenty), then commit the updated files.

Usage:
    scripts/refresh_static_data.py [--cli PATH] [--dataset bus_stops|bus_routes|all]

The CLI defaults to ~/workspace/skills/sg-bus-mcp/bin/sg-bus.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

PAGE_SIZE = 500
DATASETS = ("bus_stops", "bus_routes")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DEFAULT_CLI = (
    Path.home() / "workspace" / "skills" / "sg-bus-mcp" / "bin" / "sg-bus"
)


def dump_page(cli: str, dataset: str, skip: int) -> dict:
    proc = subprocess.run(
        [cli, "dump-static-data", dataset, str(skip)],
        capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sg-bus failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return json.loads(proc.stdout)


def refresh(cli: str, dataset: str, jobs: int = 4) -> Path:
    # Fetch in parallel batches; a short (< PAGE_SIZE) page means we've hit
    # the end, exactly like the server's own pagination logic.
    pages: dict[int, list[dict]] = {}
    skip = 0
    while True:
        skips = [skip + i * PAGE_SIZE for i in range(jobs)]
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            results = pool.map(lambda s: (s, dump_page(cli, dataset, s)), skips)
            batch = sorted(results, key=lambda t: t[0])
        done = False
        for s, page in batch:
            records_page = page.get("records") or []
            pages[s] = records_page
            print(f"  {dataset}: got {len(records_page)} records (skip={s})",
                  flush=True)
            if len(records_page) < PAGE_SIZE:
                done = True
        if done:
            break
        skip += jobs * PAGE_SIZE
        if skip > 100_000:  # sanity cap: 200 pages
            raise RuntimeError(f"{dataset}: unexpectedly large, bump the cap")

    records = [r for s in sorted(pages) for r in pages[s]]

    out = {
        "generated_at": date.today().isoformat(),
        "source": "LTA DataMall (raw records, via dump_static_data)",
        "records": records,
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{dataset}.json"
    path.write_text(json.dumps(out, ensure_ascii=False) + "\n")
    size_kb = path.stat().st_size // 1024
    print(f"wrote {path} — {len(records)} records, {size_kb} KB")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild bundled bus datasets.")
    ap.add_argument("--cli", default=os.environ.get("SG_BUS_CLI", str(DEFAULT_CLI)))
    ap.add_argument("--dataset", choices=(*DATASETS, "all"), default="all")
    ap.add_argument("--jobs", type=int, default=4,
                    help="parallel page fetches per dataset (default 4)")
    args = ap.parse_args()

    datasets = DATASETS if args.dataset == "all" else (args.dataset,)
    for dataset in datasets:
        refresh(args.cli, dataset, jobs=args.jobs)
    print("Done. Commit the updated data/*.json files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
