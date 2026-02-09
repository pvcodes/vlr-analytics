import asyncio

from scrapers_async.scraper import stats_scrapper


async def main():
    done = await stats_scrapper()
    if not done:
        raise Exception("failed")


if __name__ == "__main__":
    asyncio.run(main())
