import asyncio

from dotenv import load_dotenv

load_dotenv()
from scrapers.scraper import stats_scrapper


async def main():
    await stats_scrapper()


if __name__ == "__main__":
    asyncio.run(main())
