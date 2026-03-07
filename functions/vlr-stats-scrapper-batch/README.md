# VLR Stats Scraper Batch

Async batch scraper for Valorant player stats from [vlr.gg](https://www.vlr.gg/stats).

## Overview

Scrapes player statistics for VCT events across different maps, regions, and agents. Data is written to CSV files in a bronze layer data lake structure.

## Data Extracted

| Field | Description |
|-------|-------------|
| player_id | VLR player identifier |
| player | Player name |
| org | Player's organization |
| agents | Agent played |
| rounds_played | Total rounds played |
| rating | Player rating |
| average_combat_score | ACS |
| kill_deaths | K/D ratio |
| average_damage_per_round | ADR |
| kills_per_round | KPR |
| headshot_percentage | HS% |
| clutch_success_percentage | Clutch win rate |
| max_kills_in_single_map | Maximum kills in a map |
| kills, deaths, assists | Raw counts |
| first_kills, first_deaths | Entry fight stats |

## Architecture

- **Async workers**: One worker per proxy for concurrent scraping
- **Proxy rotation**: Uses residential proxies to avoid rate limiting
- **Job queue**: PostgreSQL tracks pending/scrapped partitions
- **Storage**: CSV files to local path (LOCAL) or GCS bucket (PRODUCTION)

## Directory Structure

```
vlr-stats-scrapper-batch/
├── main.py              # Entry point
├── scrapers/
│   ├── scraper.py      # Worker orchestration
│   └── stats.py        # VLR stats parsing
├── utils/
│   ├── constants.py    # Configuration & mappings
│   ├── helpers.py      # DB, GCS, proxy utilities
│   ├── db.py           # PostgreSQL operations
│   ├── gcp.py          # Google Cloud utilities
│   └── vct_logging.py  # Logging setup
└── scripts/
    └── RUN.yaml        # Job configuration
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ENVIRONMENT` | Yes | `LOCAL` or `PRODUCTION` |
| `GCS_DATALAKE_BUCKET_NAME` | Production | GCS bucket for CSV output |
| `DATASET_PATH` | Local | Local path for CSV output |
| `PROXY_USER` | Yes | Proxy authentication username |
| `PROXY_PSWRD` | Yes | Proxy authentication password |
| `DB_HOST` | Yes | PostgreSQL host |
| `DB_PORT` | Yes | PostgreSQL port |
| `DB_NAME` | Yes | Database name |
| `DB_USER` | Yes | Database user |
| `DB_PASSWORD` | Yes | Database password |

## Usage

### Local Development

```bash
# Install dependencies
uv sync

# Set environment variables
cp .env.example .env
# Edit .env with your config

# Run scraper
uv run python main.py
```

### Docker

```bash
# Build image
docker build -t vlr-stats-scraper .

# Run container
docker run --env-file .env.prod vlr-stats-scraper
```

### Cloud Run Deployment

```bash
# Deploy to Cloud Run
gcloud run deploy vlr-stats-scraper-batch \
  --source . \
  --region asia-south1 \
  --service-account vlr-scraper-sa@vlr-analytics.iam.gserviceaccount.com
```

### Schedule Job

```bash
# Create Cloud Scheduler job (runs every 40 minutes)
gcloud scheduler jobs create http vlr-analytics-scraper-scheduler \
  --location=asia-south1 \
  --schedule="*/40 * * * *" \
  --uri="https://run.googleapis.com/v2/projects/vlr-analytics/locations/asia-south1/jobs/vlr-stats-scraper-batch:run" \
  --http-method=POST \
  --message-body="{}" \
  --oauth-service-account-email=vlr-scheduler-sa@vlr-analytics.iam.gserviceaccount.com
```

## Output Format

CSV files written to:

```
bronze/
└── event_id={event_id}/
    └── region={region}/
        └── map={map_name}/
            └── agent={agent}/
                └── snapshot_date={date}/
                    └── data.csv
```
