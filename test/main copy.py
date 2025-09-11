import pandas as pd
from config import years, leagues, categories, categories_match, standard_url
from pipeline.url_builder import build_leagueinfo
from scrappers.scrappers import combine_data, combine_match_data
from scrappers.scrappers import get_player_links, get_match_logs
import os

def main():



    #Get player links
    player_links = get_player_links(years, standard_url)

    # Limit to first 5 players for testing
    #player_links = player_links[:5]

    #Get match logs only for those 5 players
    match_logs = get_match_logs(years, player_links)
    if match_logs:
        match_logs_df = pd.concat(match_logs, ignore_index=True)
        #print(match_logs_df.head())
    else:
        print("No match logs retrieved")

    #Save to CSV inside data folder
    os.makedirs("data/raw", exist_ok=True)
    output_path = os.path.join("data", "raw", "player_match_logs.csv") 

    match_logs_df.to_csv(output_path, index=False)
    print(f"Match logs saved to {output_path}")


if __name__ == "__main__":
    main()
