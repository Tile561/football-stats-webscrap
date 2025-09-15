import pandas as pd
from config import years, leagues, categories, categories_match, standard_url, stats
from pipeline.url_builder import build_leagueinfo
from scrappers.scrappers import combine_data, combine_match_data
from scrappers.scrappers import get_player_links, get_match_logs
import os

def main():

    player_links = get_player_links(years, standard_url)

    #player_links = player_links[:5]
    #print(player_links)
    all_match_logs = get_match_logs(years, player_links, stats)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data", "raw")
    os.makedirs(data_path, exist_ok=True)

    for stat, df in all_match_logs.items():
        output_path = os.path.join(data_path, f"player_match_logs_{stat}_raw.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved {stat} match logs to {output_path}")



if __name__ == "__main__":
    main()


'''
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
        if isinstance(dfs, list):  
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
            else:
                print("No data found")
                continue
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/raw_{name}.csv", index=False)

    # Save match-related categories
    for name, dfs in dataframes_match.items():
        if isinstance(dfs, list):
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
            else:
                print("No data found")
                continue
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/raw_{name}.csv", index=False)
'''