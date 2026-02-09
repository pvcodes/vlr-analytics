import httpx
from selectolax.parser import HTMLParser

from utils.config import VLR_BASE_URL, VLR_REQUEST_HEADER


async def vlr_stats(
    event_group_id: str,
    VLR_REGIONS_DICT: str,
    agent: str,
    map_id: str,
    min_rounds: str = 0,
    min_rating: str = 0,
    timespan: str = "all",
):
    url = f"{VLR_BASE_URL}/stats"
    params = {
        "event_group_id": event_group_id,
        "VLR_REGIONS_DICT": VLR_REGIONS_DICT,
        "agent": agent,
        "map_id": map_id,
        "min_rounds": min_rounds,
        "min_rating": min_rating,
        "timespan": "all" if timespan.lower() == "all" else f"{timespan}d",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url, params=params, headers=VLR_REQUEST_HEADER, timeout=None
        )

        if resp.status_code > 299:
            raise Exception(f"Request throwed: {resp.status_code}")

    html = HTMLParser(resp.text)

    result = []
    for item in html.css("tbody tr"):
        player = item.text().replace("\t", "").replace("\n", " ").strip().split()
        player_name = player[0]
        org = player[1] if len(player) > 1 else "N/A"

        agents = [
            agents.attributes["src"].split("/")[-1].split(".")[0]
            for agents in item.css("td.mod-agents img")
        ]
        color_sq = [stats.text() for stats in item.css("td.mod-color-sq")]
        rnd = item.css_first("td.mod-rnd").text()
        clutches_won_played_ratio = item.css_first("td.mod-cl").text().strip()
        max_kills_in_single_map = item.css_first("td.mod-kmax").text().strip()
        # print(player_name, clutches_won_played_ratio, max_kills_in_single_map)
        player_id = (
            item.css_first("td:first-child a").attributes.get("href").split("/")[2]
        )
        naked_stats = [stats.text().strip() for stats in item.css("td")[-5:]]
        # print(naked_stats)
        # return

        result.append(
            {
                "player_id": player_id,
                "player": player_name,
                "org": org,
                "agents": agents[0],
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
