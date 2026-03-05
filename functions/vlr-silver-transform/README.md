# VLR Silver Tranformation

A pyspark job for making the bronze layer cleaned and conformed data.

## About data

The bronze layer data is partitioned by: event_id, region, map, agent, snapshot_date

Structure is

```
event_id=76/
  region=cn/
    map=abyss/
      agent=kayo/
        snapshot_date=2026-02-28/
          data.csv

```

## And the csv contains following columns

- `player_id`, `player`, `org`, `agents`
- `rounds_played`, `rating`, `average_combat_score`
- `kill_deaths`, `kill_assists_survived_traded`
- `average_damage_per_round`, `kills_per_round`, `assists_per_round`
- `first_kills_per_round`, `first_deaths_per_round`
- `headshot_percentage`, `clutch_success_percentage`, `clutches_won_played_ratio`
- `max_kills_in_single_map`, `kills`, `deaths`, `assists`
- `first_kills`, `first_deaths`
- partitioned by: event_id, region, map, agent, snapshot_date

#### Overview

| Field Name                     | Description                                                                                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------|
| `player_id`                    | Unique identifier for the player extracted from the player profile URL.                                                                          |
| `player`                       | In-game name of the player.                                                                                                                      |
| `org`                          | Organization or team the player represents during the event.                                                                                     |
| `rounds_played`                | Total number of rounds the player participated in for the given `event_id`, `region`, `map`, and `agent`.                                        |
| `rating`                       | Player rating (VLR Rating 2.0) summarizing performance for the given `event_id`, `region`, `map`, and `agent`.                                   |
| `average_combat_score`         | Average Combat Score (ACS) per round for the given `event_id`, `region`, `map`, and `agent`.                                                     |
| `kill_deaths`                  | Kill-to-death ratio (kills divided by deaths) for the given `event_id`, `region`, `map`, and `agent`.                                            |
| `kill_assists_survived_traded` | Percentage of rounds where the player had a kill, assist, survived, or was traded (KAST) for the given `event_id`, `region`, `map`, and `agent`. |
| `average_damage_per_round`     | Average damage dealt per round (ADR) for the given `event_id`, `region`, `map`, and `agent`.                                                     |
| `kills_per_round`              | Average number of kills per round (KPR) for the given `event_id`, `region`, `map`, and `agent`.                                                  |
| `assists_per_round`            | Average number of assists per round (APR) for the given `event_id`, `region`, `map`, and `agent`.                                                |
| `first_kills_per_round`        | Average number of first kills per round (FKPR) for the given `event_id`, `region`, `map`, and `agent`.                                           |
| `first_deaths_per_round`       | Average number of first deaths per round (FDPR) for the given `event_id`, `region`, `map`, and `agent`.                                          |
| `headshot_percentage`          | Percentage of kills that were headshots for the given `event_id`, `region`, `map`, and `agent`.                                                  |
| `clutch_success_percentage`    | Percentage of clutch situations successfully won for the given `event_id`, `region`, `map`, and `agent`.                                         |
| `clutches_won_played_ratio`    | Ratio of clutches won to clutches attempted for the given `event_id`, `region`, `map`, and `agent`.                                              |
| `max_kills_in_single_map`      | Maximum kills achieved by the player in a single map within the given `event_id`, `region`, `map`, and `agent`.                                  |
| `kills`                        | Total number of kills for the given `event_id`, `region`, `map`, and `agent`.                                                                    |
| `deaths`                       | Total number of deaths for the given `event_id`, `region`, `map`, and `agent`.                                                                   |
| `assists`                      | Total number of assists for the given `event_id`, `region`, `map`, and `agent`.                                                                  |
| `first_kills`                  | Total number of first kills secured for the given `event_id`, `region`, `map`, and `agent`.                                                      |
| `first_deaths`                 | Total number of first deaths recorded for the given `event_id`, `region`, `map`, and `agent`.                                                    |

## Transformation performed

Adding columns:

1. `_source_file` as the path of source
2. `_ingested_at` ingestion data for silver layer

Dropping column     : `agents` redudant as it is already in partitioning

1. Dedup on COMPOSITE_KEY = ["player_id", "snapshot_date", "agent", "map", "event_id", "region"]
2.
