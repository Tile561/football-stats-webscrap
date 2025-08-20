from scrape_stats_function import combine_data
import pandas as pd
from scrape_stats_function import combine_match_data

years = ["2024-2025","2023-2024","2022-2023","2021-2022","2020-2021","2019-2020","2018-2019","2017-2018","2016-2017","2015-2016","2014-2015","2013-2014","2012-2013","2011-2012","2010-2011"]

leagues = [
    {"name": "Bundesliga", "id": 20},
    {"name": "Premier League", "id": 9},
    {"name": "La Liga", "id": 12},
    {"name": "Serie A", "id": 11},
    {"name": "Ligue 1", "id": 13},
    {"name": "Eredivisie", "id": 23},
    {"name": "Primeira Liga", "id": 32},
    {"name": "Championship", "id": 10},
    {"name": "Scottish Premiership", "id": 40},
]


# stat categories
categories = {
    "gca": [
        ("player_gca_df", "stats_gca"),
        ("team_gca_df", "stats_squads_gca_for"),
        ("team_gca_against_df", "stats_squads_gca_against"),
    ],
    "keepersadv": [
        ("player_keepersadv_df", "stats_keeper_adv"),
        ("team_keepersadv_df", "stats_squads_keeper_adv_for"),
        ("team_keepersadv_against_df", "stats_squads_keeper_adv_against"),
    ],
    "shooting": [
        ("player_shooting_df", "stats_shooting"),
        ("team_shooting_df", "stats_squads_shooting_for"),
        ("team_shooting_against_df", "stats_squads_shooting_against"),
    ],
    "passing": [
        ("player_passing_df", "stats_passing"),
        ("team_passing_df", "stats_squads_passing_for"),
        ("team_passing_against_df", "stats_squads_passing_against"),
    ],
    "passing_types": [
        ("player_passing_type_df", "stats_passing_types"),
        ("team_passing_type_df", "stats_squads_passing_types_for"),
        ("team_passing_type_against_df", "stats_squads_passing_types_against"),
    ],
    "defense": [
        ("player_defense_df", "stats_defense"),
        ("team_defense_df", "stats_squads_defense_for"),
        ("team_defense_against_df", "stats_squads_defense_against"),
    ],
    "possession": [
        ("player_possession_df", "stats_possession"),
        ("team_possession_df", "stats_squads_possession_for"),
        ("team_possession_against_df", "stats_squads_possession_against"),
    ],
    "standard": [
        ("stats_player_standard_df", "stats_standard"),
        ("stats_squad_standard_df", "stats_squads_standard_for"),
        ("stats_squad_standard_against_df", "stats_squads_standard_against"),
    ],
    "keeper": [
        ("stats_keeper_df", "stats_keeper"),
    ],
}


categories_match = {
    "league": [
            ("league_finish_df", None),
        ],
        "schedule": [
            ("schedule_df", None),
        ]
}

def build_leagueinfo(leagues, category):
    leagueinfo = []
    for league in leagues:
        leagueinfo.append({
            "name": league["name"],
            "url": f"https://fbref.com/en/comps/{league['id']}/{{year}}/{category}/{{year}}-{league['name'].replace(' ', '-')}-Stats"
        })
    return leagueinfo


dataframes = {}
for category, tables in categories.items():
    leagueinfo = build_leagueinfo(leagues, category)
    for name, table_id in tables:
        dfs = combine_data(years, leagueinfo, table_id=table_id)  
        dataframes[name] = dfs  


dataframes_match = {}
for category, tables in categories_match.items():
    leagueinfo = build_leagueinfo(leagues, category)
    for name, table_id in tables:
        dfs = combine_match_data(years, leagueinfo, table_id=table_id) 
        dataframes_match[name] = dfs