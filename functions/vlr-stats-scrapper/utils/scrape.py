import httpx

from selectolax.parser import HTMLParser
from typing import List, Dict

from utils.constants import VLR_BASE_URL, VLR_REQUEST_HEADER

timeout = httpx.Timeout(connect=10.0, timeout=20.0)

client = httpx.Client(
    headers=VLR_REQUEST_HEADER,
    timeout=timeout,
    follow_redirects=True,
)


def vlr_stats(
    event_group_id: str,
    region: str,
    agent: str,
    map_id: str,
    min_rounds: int = 0,
    min_rating: float = 0,
    timespan: str = "all",
) -> List[Dict]:
    url = f"{VLR_BASE_URL}/stats"

    params = {
        "event_group_id": event_group_id,
        "region": region,
        "agent": agent,
        "map_id": map_id,
        "min_rounds": min_rounds,
        "min_rating": min_rating,
        "timespan": "all" if timespan.lower() == "all" else f"{timespan}d",
    }

    try:
        resp = client.get(url, params=params)
    except httpx.RequestError as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc

    if resp.status_code >= 300:
        raise RuntimeError(f"Request failed with status: {resp.status_code}")

    html = HTMLParser(resp.text)

    result: List[Dict] = []

    for item in html.css("tbody tr"):
        player = item.text().replace("\t", "").replace("\n", " ").strip().split()

        if not player:
            continue

        player_name = player[0]
        org = player[1] if len(player) > 1 else "N/A"

        agents = [
            agents.attributes["src"].split("/")[-1].split(".")[0]
            for agents in item.css("td.mod-agents img")
        ]

        color_sq = [stats.text() for stats in item.css("td.mod-color-sq")]

        rnd_node = item.css_first("td.mod-rnd")
        cl_node = item.css_first("td.mod-cl")
        kmax_node = item.css_first("td.mod-kmax")
        id_node = item.css_first("td:first-child a")

        if not (rnd_node and cl_node and kmax_node and id_node):
            continue  # Skip malformed rows safely

        rnd = rnd_node.text()
        clutches_won_played_ratio = cl_node.text().strip()
        max_kills_in_single_map = kmax_node.text().strip()

        player_id = id_node.attributes.get("href", "").split("/")[2]

        naked_stats = [stats.text().strip() for stats in item.css("td")[-5:]]

        if len(color_sq) < 11 or len(naked_stats) < 5:
            continue  # Protect against layout changes

        result.append(
            {
                "player_id": player_id,
                "player": player_name,
                "org": org,
                "agents": agents[0] if agents else "N/A",
                "rounds_played": rnd,
                "rating": color_sq[0],
                "average_combat_score": color_sq[1],
                "kill_deaths": color_sq[2],
                "kill_assists_survived_traded": color_sq[3],
                "average_damage_per_round": color_sq[4],
                "kills_per_round": color_sq[5],
                "assists_per_round": color_sq[6],
                "first_kills_per_round": color_sq[7],
                "first_deaths_per_round": color_sq[8],
                "headshot_percentage": color_sq[9],
                "clutch_success_percentage": color_sq[10],
                "clutches_won_played_ratio": clutches_won_played_ratio,
                "max_kills_in_single_map": max_kills_in_single_map,
                "kills": naked_stats[0],
                "deaths": naked_stats[1],
                "assists": naked_stats[2],
                "first_kills": naked_stats[3],
                "first_deaths": naked_stats[4],
            }
        )

    return result
