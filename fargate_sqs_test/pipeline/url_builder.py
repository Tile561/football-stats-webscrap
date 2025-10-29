from config import player_matchlog_categories


def build_leagueinfo(leagues, category):
    leagueinfo = []
    for league in leagues:
        leagueinfo.append({
            "name": league["name"],
            "url": f"https://fbref.com/en/comps/{league['id']}/{{year}}/{category}/{{year}}-{league['name'].replace(' ', '-')}-Stats"
        })
    return leagueinfo


def create_links(years, leagues):
    links = []
    for league in leagues:
        for year in years:
            url = league["url"].format(year=year)
            links.append(url)
    return links

def make_matchlog_links(player_urls, years, stats):
    links = {stat: [] for stat in stats}
    seen = set()   

    for url in player_urls:
        parts = url.rstrip("/").split("/")
        if len(parts) < 2:
            continue
        player_id = parts[-2]
        player_slug = parts[-1]

        for year in years:
            for stat in player_matchlog_categories:
                key = (player_id, year, stat)
                if key in seen:
                    continue
                seen.add(key)

                matchlog_url = (
                    f"https://fbref.com/en/players/{player_id}/matchlogs/"
                    f"{year}/{stat}/{player_slug}-Match-Logs"
                )

                links[stat].append({
                    "name": f"{player_slug} - {stat} {year}",
                    "url": matchlog_url,
                    "player_id": player_id,
                    "player_slug": player_slug,
                    "year": year,
                    "stat": stat
                })
    return links
