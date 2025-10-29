
HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE = "https://fbref.com"
years = ["2024-2025"]
#,"2023-2024","2022-2023","2021-2022","2020-2021","2019-2020","2018-2019","2017-2018","2016-2017","2015-2016","2014-2015","2013-2014","2012-2013","2011-2012","2010-2011"
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
    {"name": "Belgian Pro League", "id": 37},     # Belgium
    {"name": "Danish Superliga", "id": 50},       # Denmark
    {"name": "Swiss Super League", "id": 57},     # Switzerland
    {"name": "Austrian Bundesliga", "id": 56},    # Austria
    {"name": "Turkish Süper Lig", "id": 26},      # Turkey
]
standard_url = [
    {
        "name": "Bundesliga",
        "url": "https://fbref.com/en/comps/20/{year}/stats/{year}-Bundesliga-Stats"
    },

   
]

'''
     {
        "name": "Premier League",
        "url": "https://fbref.com/en/comps/9/{year}/stats/{year}-Premier-League-Stats"
    },
    {
        "name": "La Liga",
        "url": "https://fbref.com/en/comps/12/{year}/stats/{year}-La-Liga-Stats"
    },
    {
        "name": "Serie A",
        "url": "https://fbref.com/en/comps/11/{year}/stats/{year}-Serie-A-Stats"
    },
    {
        "name": "Ligue 1",
        "url": "https://fbref.com/en/comps/13/{year}/stats/{year}-Ligue-1-Stats"
    },
    {
        "name": "Eredivisie",
        "url": "https://fbref.com/en/comps/23/{year}/stats/{year}-Eredivisie-Stats"
    },
    {
        "name": "Primeira Liga",
        "url": "https://fbref.com/en/comps/32/{year}/stats/{year}-Primeira-Liga-Stats"
    },
    {
        "name": "Championship",
        "url": "https://fbref.com/en/comps/10/{year}/stats/{year}-EFL-Championship-Stats"
    },
    {
        "name": "Scottish Premiership",
        "url": "https://fbref.com/en/comps/40/{year}/stats/{year}-Scottish-Premiership-Stats"
    },
    {
        "name": "Belgian Pro League",
        "url": "https://fbref.com/en/comps/37/{year}/stats/{year}-Belgian-Pro-League-Stats"
    },
    {
        "name": "Danish Superliga",
        "url": "https://fbref.com/en/comps/50/{year}/stats/{year}-Danish-Superliga-Stats"
    },
    {
        "name": "Swiss Super League",
        "url": "https://fbref.com/en/comps/57/{year}/stats/{year}-Swiss-Super-League-Stats"
    },
    {
        "name": "Austrian Bundesliga",
        "url": "https://fbref.com/en/comps/56/{year}/stats/{year}-Austrian-Bundesliga-Stats"
    },
    {
        "name": "Turkish Süper Lig",
        "url": "https://fbref.com/en/comps/26/{year}/stats/{year}-Super-Lig-Stats"
    }

'''
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
    "stats": [
        ("stats_player_standard_df", "stats_standard"),
        ("stats_squad_standard_df", "stats_squads_standard_for"),
        ("stats_squad_standard_against_df", "stats_squads_standard_against"),
    ],
    "keepers": [
        ("stats_keeper_df", "stats_keeper"),
        ("stats_squad_keeper_df", "stats_squads_keeper_for"),
        ("stats_squad_keeper_against_df", "stats_squads_keeper_against"),
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

# Drop rules for player and team DataFrames
drop_rules = {
    'eredivisie': list(range(2010, 2018)),
    'championship': list(range(2010, 2018)),
    'primeira liga': list(range(2010, 2018)),
    'bundesliga': list(range(2010, 2017)),
    'premier league': list(range(2010, 2017)),
    'la liga': list(range(2010, 2017)),
    'serie a': list(range(2010, 2017)),
    'ligue 1': list(range(2010, 2017)),
    'scottish premiership': list(range(2010, 2026))
}

team_drop_rules = {
    'eredivisie': list(range(2010, 2018)),
    'championship': list(range(2010, 2018)),
    'primeira liga': list(range(2010, 2018)),
    'bundesliga': list(range(2010, 2017)),
    'premier league': list(range(2010, 2017)),
    'la liga': list(range(2010, 2017)),
    'serie a': list(range(2010, 2017)),
    'ligue 1': list(range(2010, 2017)),
    'scottish premiership': list(range(2010, 2026))
}

against_drop_rules = {
    'eredivisie': list(range(2010, 2017)),
    'championship': list(range(2010, 2016)),
    'primeira liga': list(range(2010, 2016)),
    'bundesliga': list(range(2010, 2016)),
    'premier league': list(range(2010, 2016)),
    'la liga': list(range(2010, 2015)),
    'serie a': list(range(2010, 2015)),
    'ligue 1': list(range(2010, 2015)),
    'scottish premiership': list(range(2010, 2021))
}


exclude_tables = {
    "raw_stats_player_standard_df",  
    "raw_player_keeper_df",
    "raw_player_shooting_df",   
}

exclude_team_tables = {
    "raw_stats_team_standard_df",
    "raw_stats_team_shooting_df",
    "raw_team_keeper_df", 
}

exclude_tables_against = {
    "raw_stats_team_standard_against_df",
    "raw_team_shooting_against_df",
    "raw_team_keeper_against_df",
}

team_tables = exclude_team_tables.union(exclude_tables_against)
against_tables = {table for table in team_tables if "against" in table}


player_matchlog_categories = ["summary","defense","passing","gca","possession","passing_types","keeper"]
