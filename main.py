import asyncio

from dotenv import load_dotenv

load_dotenv()
from scrapers_async.scraper import stats_scrapper as async_stats_scrapper
from scrapers.scraper import stats_scrapper

# from scrapers.scraper import stats_scrapper


async def main():
    # upload_bronze_data()
    # list_all_local_data()
    # return

    await async_stats_scrapper()


if __name__ == "__main__":
    stats_scrapper()
    # asyncio.run(main())
