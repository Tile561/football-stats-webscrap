
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

def make_matchlog_links(player_urls, years):
    links = []
    for url in player_urls:
        parts = url.strip("/").split("/")
        player_id = parts[-2]
        player_slug = parts[-1]
        for year in years:
            matchlog_url = f"https://fbref.com/en/players/{player_id}/matchlogs/{year}/{player_slug}-Match-Logs"
            links.append({
                "name": f"{player_slug}",
                "url": matchlog_url
            })
    return links