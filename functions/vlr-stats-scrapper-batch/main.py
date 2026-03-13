import asyncio
from utils.constants import fetch_proxies


from scrapers.scraper import stats_scrapper
from utils.constants import SCRAPER_INSTANCE_COUNT


PROXIES = fetch_proxies()


async def main():
    tasks = [
        stats_scrapper(proxy["user"], proxy["password"], instance_index=i)
        for i, proxy in enumerate(PROXIES[:SCRAPER_INSTANCE_COUNT])
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
