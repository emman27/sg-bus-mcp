#!/usr/bin/env python3
"""Build data/mrt_stations.json for the sg-bus-mcp repo.

Source: github.com/ayaka14732/singapore-hdb-map data/mrt_network.json and
data/lrt_network.json (station codes/coords/line topology), themselves
derived from LTA DataMall, the LTA system map and data.gov.sg under the
Singapore Open Data Licence. Re-run when new stations open.
"""

import json
import sys
import urllib.request
from pathlib import Path

BASE = "https://raw.githubusercontent.com/ayaka14732/singapore-hdb-map/main/data"
OUT = Path(__file__).resolve().parents[1] / "data" / "mrt_stations.json"

# ayaka line id -> LTA PCD/TrainServiceAlerts line code
LINE_CODES = {
    "NS": "NSL", "EW": "EWL", "NE": "NEL", "CC": "CCL",
    "DT": "DTL", "TE": "TEL", "BP": "BPL", "SK": "SLRT", "PG": "PLRT",
}


def fetch(name: str) -> dict:
    with urllib.request.urlopen(f"{BASE}/{name}", timeout=60) as resp:
        return json.load(resp)


def main() -> None:
    mrt = fetch("mrt_network.json")
    lrt = fetch("lrt_network.json")

    lines: dict[str, dict] = {}
    for net in (mrt, lrt):
        for line in net["lines"]:
            lta = LINE_CODES[line["id"]]
            ordered: list[str] = []
            for seg in line["segments"]:
                for code in seg:
                    if code not in ordered:
                        ordered.append(code)
            lines[lta] = {
                "name": line["name"],
                "color": line.get("color"),
                "codes": ordered,
                "segments": line["segments"],
            }

    stations = []
    for net in (mrt, lrt):
        for s in net["stations"]:
            stations.append({
                "name": s["name"],
                "codes": s["codes"],
                "lines": [LINE_CODES[lid] for lid in s["lines"]],
                "lat": round(s["lat"], 7),
                "lon": round(s["lng"], 7),
            })

    out = {
        "generated_at": "2026-09-25",
        "source": (
            "Derived from github.com/ayaka14732/singapore-hdb-map "
            f"data/mrt_network.json (updated {mrt.get('updated')}) and "
            f"data/lrt_network.json (updated {lrt.get('updated')}), themselves "
            "sourced from LTA DataMall, the LTA system map and data.gov.sg "
            "under the Singapore Open Data Licence."
        ),
        "lines": lines,
        "stations": stations,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    print(f"wrote {OUT}: {len(lines)} lines, {len(stations)} stations")


if __name__ == "__main__":
    sys.exit(main())
