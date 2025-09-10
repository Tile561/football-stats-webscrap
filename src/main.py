import pandas as pd
from config import years, leagues, categories, categories_match, standard_url
from pipeline.url_builder import build_leagueinfo
from scrappers.scrappers import combine_data, combine_match_data
from scrappers.scrappers import get_player_links, get_match_logs
import os

def main():

    # Stats categories
    dataframes = {}
    for category, tables in categories.items():
        leagueinfo = build_leagueinfo(leagues, category)
        for name, table_id in tables:
            dfs = combine_data(years, leagueinfo, table_id=table_id)  
            dataframes[name] = dfs  

    # Match-related categories
    dataframes_match = {}
    for category, tables in categories_match.items():
        leagueinfo = build_leagueinfo(leagues, category)
        for name, table_id in tables:
            dfs = combine_match_data(years, leagueinfo, table_id=table_id) 
            dataframes_match[name] = dfs  

    # Save stats categories
    for name, dfs in dataframes.items():
        if isinstance(dfs, list):  # combine list of DataFrames
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/{name}_test.csv", index=False)

    # Save match-related categories
    for name, dfs in dataframes_match.items():
        if isinstance(dfs, list):
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/{name}_test.csv", index=False)


    # 1. Get player links
    player_links = get_player_links(years, standard_url)

    # Limit to first 5 players for testing
    #player_links = player_links[:5]

    # 2. Get match logs only for those 5 players
    match_logs = get_match_logs(years, player_links)
    match_logs_df = pd.concat(match_logs, ignore_index=True)
    #print(match_logs_df.head())

    # 3. Save to CSV inside data folder
    os.makedirs("data/raw", exist_ok=True)
    output_path = os.path.join("data", "raw", "player_match_logs.csv") 

    match_logs_df.to_csv(output_path, index=False)
    print(f"Match logs saved to {output_path}")


if __name__ == "__main__":
    main()
