# VCT Analytics — Bronze to Silver Transformation

## Overview

This pipeline transforms raw scraped VLR.gg player statistics from the **Bronze layer** (CSV) into a clean, typed, analysis-ready **Silver layer** (Parquet).

The data represents aggregated player performance stats per agent, map, region, and event — captured at a point in time via a scheduled scrape (snapshot). Since VLR.gg serves cumulative aggregations, each snapshot reflects the player's total stats up to that date, not just a single match.

---

## Data Flow

```
Bronze (CSV, raw)
  └── event_id={id}/region={region}/map={map}/agent={agent}/snapshot_date={YYYY-MM-DD}/data.csv
        │
        │  vlr-silver-transform (PySpark)
        ▼
Silver (Parquet, clean)
  └── event_id={id}/region={region}/map={map}/agent={agent}/snapshot_date={YYYY-MM-DD}/
```

---

## Source Data

- **Origin:** VLR.gg (scraped)
- **Granularity:** One row = one player's aggregated stats for a specific agent + map + region + event at a snapshot in time
- **Trigger:** Airflow DAG → Dataproc Serverless Spark Batch

### Input Schema (Bronze)

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | integer | VLR.gg player identifier |
| `player` | string | Player gamertag |
| `org` | string | Team/organisation short code |
| `agents` | string | Agent played (dropped — see partition column `agent`) |
| `rounds_played` | integer | Total rounds played |
| `rating` | double | VLR.gg player rating |
| `average_combat_score` | double | ACS |
| `kill_deaths` | double | K/D ratio |
| `kill_assists_survived_traded` | string | KAST % e.g. `"89%"` |
| `average_damage_per_round` | double | ADR |
| `kills_per_round` | double | KPR |
| `assists_per_round` | double | APR |
| `first_kills_per_round` | double | FKPR |
| `first_deaths_per_round` | double | FDPR |
| `headshot_percentage` | string | HS% e.g. `"39%"` |
| `clutch_success_percentage` | string | Clutch success % e.g. `"67%"` or NULL |
| `clutches_won_played_ratio` | string | Clutch ratio e.g. `"2/3"` |
| `max_kills_in_single_map` | integer | KMAX |
| `kills` | integer | Total kills |
| `deaths` | integer | Total deaths |
| `assists` | integer | Total assists |
| `first_kills` | integer | Total first kills |
| `first_deaths` | integer | Total first deaths |

### Partition Columns (auto-promoted from path)

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | integer | Event identifier |
| `region` | string | Region e.g. `na`, `kr`, `eu` |
| `map` | string | Map name e.g. `haven`, `bind` |
| `agent` | string | Agent name e.g. `omen`, `jett` |
| `snapshot_date` | date | Date the data was scraped |

---

## Transformation Steps

### Step 1 — Read & Deduplication

- Read all CSVs recursively from Bronze using `basePath` to auto-promote Hive partition columns
- Drop redundant `agents` CSV column — `agent` partition column is the source of truth
- Capture `_source_file` via `input_file_name()` immediately at read time (before any transformations)
- Apply defensive deduplication on the composite natural key:

```
player_id + snapshot_date + agent + map + event_id + region
```

A player legitimately appears multiple times per snapshot (different agent, map, region combinations) — only exact matches on all six fields are considered duplicates. Latest ingested row wins.

### Step 2 — Percentage Normalization & Null Handling

**Percentage columns** — strip `%` suffix and normalize to 0–1 float:

| Column | Before | After |
|--------|--------|-------|
| `kill_assists_survived_traded` | `"89%"` | `0.89` |
| `headshot_percentage` | `"39%"` | `0.39` |
| `clutch_success_percentage` | `"67%"` / NULL | `0.67` / NULL |

**Derived columns** — computed from raw counts rather than filled:

| Column | Condition | Derivation |
|--------|-----------|------------|
| `kill_deaths` | NULL when `deaths = 0` | `kills` (perfect KD, no deaths) |
| `first_kills_per_round` | NULL when `first_kills = 0` | `0.0` (0 first kills is a valid stat) |

**Null policy** — all other NULLs are preserved as NULL:

| Column | Reason NULLs are kept |
|--------|----------------------|
| `rating` | VLR.gg proprietary formula, cannot recompute |
| `kill_assists_survived_traded` | Requires round-level data |
| `average_damage_per_round` | Requires damage logs |
| `headshot_percentage` | Requires shot-level data |
| `first_deaths_per_round` | Requires round-level data |

### Step 3 — Clutch Ratio Parsing

`clutches_won_played_ratio` (e.g. `"2/3"`) is split into three columns:

| Column | Type | Description |
|--------|------|-------------|
| `clutches_won` | integer | Clutches won |
| `clutches_played` | integer | Clutch opportunities |
| `clutch_success_rate` | double | Derived: `clutches_won / clutches_played` |

`clutch_success_percentage` is dropped — replaced by the more reliable derived `clutch_success_rate`.

> **Note:** `clutch_success_percentage` from VLR.gg is NULL whenever `clutches_won = 0`, making it unreliable. The derived column correctly returns `0.0` in those cases.

### Step 4 — String Normalization

Only defensive `trim()` applied to `player` and `org`. Casing is **intentionally preserved** — player gamertags and org codes are identity fields, not free text.

### Step 5 — Lineage Metadata

Three lineage columns added to every row:

| Column | Value | Purpose |
|--------|-------|---------|
| `_ingested_at` | `current_timestamp()` | When the row was processed |
| `_source_file` | Full Bronze file path | Traceability back to source |
| `_pipeline_version` | `"1.0.0"` | Pipeline version that produced this row |

---

## Output Schema (Silver)

| Column | Type | Nullable |
|--------|------|----------|
| `player_id` | integer | No |
| `player` | string | Yes |
| `org` | string | Yes |
| `rounds_played` | integer | Yes |
| `rating` | double | Yes |
| `average_combat_score` | double | Yes |
| `kill_deaths` | double | Yes |
| `kill_assists_survived_traded` | double | Yes |
| `average_damage_per_round` | double | Yes |
| `kills_per_round` | double | Yes |
| `assists_per_round` | double | Yes |
| `first_kills_per_round` | double | Yes |
| `first_deaths_per_round` | double | Yes |
| `headshot_percentage` | double | Yes |
| `max_kills_in_single_map` | integer | Yes |
| `kills` | integer | Yes |
| `deaths` | integer | Yes |
| `assists` | integer | Yes |
| `first_kills` | integer | Yes |
| `first_deaths` | integer | Yes |
| `clutches_won` | integer | Yes |
| `clutches_played` | integer | Yes |
| `clutch_success_rate` | double | Yes |
| `event_id` | integer | Yes |
| `region` | string | Yes |
| `map` | string | Yes |
| `agent` | string | Yes |
| `snapshot_date` | date | Yes |
| `_ingested_at` | timestamp | No |
| `_source_file` | string | No |
| `_pipeline_version` | string | No |

---

## Natural Key

```
player_id + snapshot_date + agent + map + event_id + region
```

A row is uniquely identified by all six fields. A player can appear multiple times per snapshot across different agent/map/region/event combinations.

---

## Null Reference

| Column | Null Count (observed) | Meaning |
|--------|----------------------|---------|
| `rating` | 3,698 | VLR.gg did not compute — insufficient data |
| `kill_assists_survived_traded` | 3,698 | Same rows as rating — incomplete scrape |
| `clutch_success_percentage` | 6,097 | No clutch opportunities OR won 0 clutches |
| `average_damage_per_round` | 1,448 | Not available for this player/map combo |
| `headshot_percentage` | 1,582 | Not available for this player/map combo |
| `first_deaths_per_round` | 1,572 | Not available for this player/map combo |
| `clutches_won_played_ratio` | 2,261 | No clutch data recorded |

---

## Snapshot Behaviour

Each snapshot is a **full re-scrape** of cumulative stats — not a delta. Between two snapshots, new matches may have been played, causing all cumulative stats (`kills`, `rounds_played`, etc.) to increase. Stats should be monotonically non-decreasing across snapshots for the same player/agent/map/event/region combination.

```
snapshot_date=2024-01-01  →  kills=22, rounds_played=18
snapshot_date=2024-01-05  →  kills=45, rounds_played=35  ← new matches played
snapshot_date=2024-01-10  →  kills=45, rounds_played=35  ← no new matches
```

---

## Infrastructure

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow |
| Compute | GCP Dataproc Serverless Spark |
| Storage | Google Cloud Storage (GCS) |
| File format (Bronze) | CSV |
| File format (Silver) | Parquet |
| Partitioning | Hive-style: `event_id/region/map/agent/snapshot_date` |

---

## Running Locally

```bash
pip install pyspark

python transform.py \
  --base_path /path/to/bronze \
  --silver_path /path/to/silver \
  --pipeline_version 1.0.0
```

---

## Next Layer — Gold

Silver is the clean, typed, trusted source of truth. The Gold layer builds analytical tables on top:

- Player performance trends across snapshots
- Agent win rates per map
- Regional stat comparisons (NA vs KR vs EU)
- Event leaderboards

```bash
gcloud dataproc batches submit pyspark \
  --region=asia-south1 \
  --project=vlr-analytics \
  --service-account=vlr-silver-dp@vlr-analytics.iam.gserviceaccount.com \
  --version=2.2 \
  gs://vlr-code-bucket/vlr-silver-transform/main.py \
  -- \
  --base_path gs://vlr-data-lake/bronze \
  --silver_path gs://vlr-data-lake/silver \
  --snapshot_date 2026-02-26

  ```
