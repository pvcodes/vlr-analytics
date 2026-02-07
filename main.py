# from scrapers.stats import vlr_stats
from scrapers_async.stats import vlr_stats
from pathlib import Path
from utils.constants import (
    VCT_STATS_FIELDS,
    VLR_STATS_EVENT_SERIES_MAX_ID,
    VLR_AGENTS,
    VLR_REGIONS,
    VLR_MAPS,
    VLR_EVENTS_DICT,
)


import asyncio
import time
import csv


async def main():
    print(f"started at {time.strftime('%X')}")

    tasks = []
    dest_paths = []
    DATASET_PATH = Path.cwd() / "dataset"

    for event_id in range(1, VLR_STATS_EVENT_SERIES_MAX_ID + 1):
        for region in VLR_REGIONS:
            for map_id in VLR_MAPS.keys():
                for agent in VLR_AGENTS:
                    tasks.append(
                        vlr_stats(
                            event_group_id=event_id,
                            region=region,
                            agent=agent,
                            map_id=map_id,
                        )
                    )

                    dir_path = (
                        DATASET_PATH
                        / VLR_EVENTS_DICT[event_id]
                        / region
                        / VLR_MAPS[map_id]
                    ).resolve()

                    dir_path.mkdir(parents=True, exist_ok=True)

                    dest_paths.append(dir_path / f"{agent}.csv")

    try:
        results = await asyncio.gather(*tasks)
    except Exception as e:
        print("Bad", e)
        return

    for dest_path, result in zip(dest_paths, results):
        with open(dest_path, "w", newline="") as f:
            writer = csv.DictWriter(f, VCT_STATS_FIELDS)
            writer.writeheader()
            writer.writerows(result)

    print(f"finished at {time.strftime('%X')}")


if __name__ == "__main__":
    asyncio.run(main())
