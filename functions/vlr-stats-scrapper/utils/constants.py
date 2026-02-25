import os

APP_NAME = "VLR STATS SCRAPPER"

ENVIRONMENT = os.environ.get("ENVIRONMENT")
if ENVIRONMENT not in ("PRODUCTION", "LOCAL"):
    raise ValueError(f"Invalid or missing ENVIRONMENT: {ENVIRONMENT!r}")


VLR_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
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
