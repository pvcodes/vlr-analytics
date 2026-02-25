import os

APP_NAME = "VLR STATS SCRAPPER"

ENVIRONMENT = os.environ.get("ENVIRONMENT")
if ENVIRONMENT not in ("PRODUCTION", "LOCAL"):
    raise ValueError(f"Invalid or missing ENVIRONMENT: {ENVIRONMENT!r}")


VLR_REQUEST_HEADER = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.vlr.gg/",
    "Connection": "keep-alive",
}


VLR_BASE_URL = "https://www.vlr.gg"

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


GCS_DATALAKE_BUCKET_NAME = "vlr-data-lake"
