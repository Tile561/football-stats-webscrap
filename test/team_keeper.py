
from scrape_stats_function import combine_data
import pandas as pd
from scrape_stats_function import combine_match_data
#import mysql

pd.set_option('display.max_columns', None)
years = ["2024-2025","2023-2024","2022-2023","2021-2022","2020-2021","2019-2020","2018-2019","2017-2018","2016-2017","2015-2016","2014-2015","2013-2014","2012-2013","2011-2012","2010-2011"]

keeper_url = [
    {
        "name": "Bundesliga",
        "url": "https://fbref.com/en/comps/20/{year}/keepers/{year}-Bundesliga-Stats"
    },
    {
        "name": "Premier League",
        "url": "https://fbref.com/en/comps/9/{year}/keepers/{year}-Premier-League-Stats"
    },
    {
        "name": "La Liga",
        "url": "https://fbref.com/en/comps/12/{year}/keepers/{year}-La-Liga-Stats"
    },
    {
        "name": "Serie A",
        "url": "https://fbref.com/en/comps/11/{year}/keepers/{year}-Serie-A-Stats"
    },
    {
        "name": "Ligue 1",
        "url": "https://fbref.com/en/comps/13/{year}/keepers/{year}-Ligue-1-Stats"
    },
    {
        "name": "Eredivisie",
        "url": "https://fbref.com/en/comps/23/{year}/keepers/{year}-Eredivisie-Stats"
    },
    {
        "name": "Primeira Liga",
        "url": "https://fbref.com/en/comps/32/{year}/keepers/{year}-Primeira-Liga-Stats"
    },
    {
        "name": "EFL Championship",
        "url": "https://fbref.com/en/comps/10/{year}/keepers/{year}-EFL-Championship-Stats"
    },
    {
        "name":"Scottish Premiership",
        "url":"https://fbref.com/en/comps/40/{year}/keepers/{year}-Scottish-Premiership-Stats"
    }

]

stats_keeper= []
stats_keeper = combine_data(years, keeper_url, table_id='stats_squads_keeper_for')

stats_keeper_against= []
stats_keeper_against = combine_data(years, keeper_url, table_id='stats_squads_keeper_against')