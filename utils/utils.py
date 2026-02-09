from pathlib import Path
from utils.logging import logger
from utils.config import VCT_STATS_FIELDS
import csv


def write_csv(dest_path: Path, rows: list[dict]):
    if not rows:
        logger.warning(f"Empty result → skipping write: {dest_path}")
        return

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(dest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, VCT_STATS_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
