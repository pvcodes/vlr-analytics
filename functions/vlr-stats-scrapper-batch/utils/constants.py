from utils.vct_logging import logger
import os

# Scraping Configs
VLR_BASE_URL = "https://www.vlr.gg"

VLR_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vlr.gg/stats",
    "Upgrade-Insecure-Requests": "1",
}

VCT_STATS_FIELDS = [
    "player_id",
    "player",
    "org",
    "agents",
    "rounds_played",
    "rating",
    "average_combat_score",
    "kill_deaths",
    "kill_assists_survived_traded",
    "average_damage_per_round",
    "kills_per_round",
    "assists_per_round",
    "first_kills_per_round",
    "first_deaths_per_round",
    "headshot_percentage",
    "clutch_success_percentage",
    "clutches_won_played_ratio",
    "max_kills_in_single_map",
    "kills",
    "deaths",
    "assists",
    "first_kills",
    "first_deaths",
]

VLR_MAPS_DICT = {
    "1": "bind",
    "2": "haven",
    "3": "split",
    "5": "ascent",
    "6": "icebox",
    "8": "breeze",
    "9": "fracture",
    "10": "pearl",
    "11": "lotus",
    "12": "sunset",
    "13": "abyss",
    "14": "corrode",
}


ENVIRONMENT = os.environ.get("ENVIRONMENT", "LOCAL")
SCRAPER_BATCH_SIZE = int(os.environ.get("SCRAPER_BATCH_SIZE", 1000))
ASYNCIO_SEMAPHORE_CONCURRENCY = int(os.environ.get("ASYNCIO_SEMAPHORE_CONCURRENCY", 5))

GCS_DATALAKE_BUCKET_NAME = os.environ.get("GCS_DATALAKE_BUCKET_NAME")
DATASET_PATH = os.environ.get("DATASET_PATH")

PROXY_USER = os.environ.get("PROXY_USER")
PROXY_PSWRD = os.environ.get("PROXY_PSWRD")

DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

POOL_MIN_CONN = int(os.environ.get("DB_POOL_MIN_CONN", 1))
POOL_MAX_CONN = int(os.environ.get("DB_POOL_MAX_CONN", 3))

# =========================
# Startup Logging
# =========================
logger.info("Starting VLR scraper configuration")
logger.info(
    "Environment=%s | BatchSize=%s | Concurrency=%s",
    ENVIRONMENT,
    SCRAPER_BATCH_SIZE,
    ASYNCIO_SEMAPHORE_CONCURRENCY,
)

if PROXY_USER:
    logger.info("Proxy configured (username provided)")
else:
    logger.warning("Proxy username not set")

if PROXY_PSWRD:
    logger.info("Proxy password provided")
else:
    logger.warning("Proxy password not set")


# =========================
# Validation
# =========================
def validate_configuration():
    logger.info("Validating runtime configuration...")

    if ENVIRONMENT not in {"LOCAL", "PRODUCTION"}:
        logger.error("Invalid ENVIRONMENT value: %s", ENVIRONMENT)
        raise ValueError("ENVIRONMENT must be LOCAL or PRODUCTION")

    if ENVIRONMENT == "PRODUCTION":
        if not GCS_DATALAKE_BUCKET_NAME:
            logger.critical(
                "Missing required env var GCS_DATALAKE_BUCKET_NAME in PRODUCTION"
            )
            raise EnvironmentError("GCS_DATALAKE_BUCKET_NAME is required in PRODUCTION")

    if ENVIRONMENT == "LOCAL":
        if not DATASET_PATH:
            logger.critical("Missing required env var DATASET_PATH in LOCAL")
            raise EnvironmentError("DATASET_PATH is required in LOCAL")

    if not PROXY_USER or not PROXY_PSWRD:
        logger.critical("Proxy credentials are required but missing")
        raise EnvironmentError("PROXY_USER and PROXY_PSWRD must be set")

    if ASYNCIO_SEMAPHORE_CONCURRENCY <= 0:
        logger.error(
            "Invalid concurrency value: %s",
            ASYNCIO_SEMAPHORE_CONCURRENCY,
        )
        raise ValueError("ASYNCIO_SEMAPHORE_CONCURRENCY must be > 0")

    logger.info("Configuration validation successful")


validate_configuration()
