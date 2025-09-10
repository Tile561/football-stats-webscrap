from scrappers.scrappers import combine_data
#from config import standard_url
from scrappers.scrappers import get_player_links
import pandas as pd
years = ["2024-2025"]
import os
standard_url = [
    {
        "name": "Bundesliga",
        "url": "https://fbref.com/en/comps/20/{year}/stats/{year}-Bundesliga-Stats"
    },
]

def make_matchlog_links(player_urls, years, stats):
    links = {stat: [] for stat in stats}  # dict of lists
    for url in player_urls:
        parts = url.strip("/").split("/")
        player_id = parts[-2]
        player_slug = parts[-1]
        for year in years:
            for stat in stats:
                matchlog_url = f"https://fbref.com/en/players/{player_id}/matchlogs/{year}/{stat}/{player_slug}-Match-Logs"
                links[stat].append({
                    "name": f"{player_slug} - {stat}",
                    "url": matchlog_url
                })
    return links

def get_match_logs(years, player_links, stats):
    matchlog_links = make_matchlog_links(player_links, years, stats)
    
    all_dfs = {}
    for stat, links in matchlog_links.items():
        dfs = []
        for link in links:
            try:
                df_list = combine_data(years, [link], table_id="matchlogs_all")  # may return a list
                if df_list:  
                    # flatten if df_list is already a list of DataFrames
                    if isinstance(df_list, list):
                        dfs.extend(df_list)
                    else:
                        dfs.append(df_list)
            except Exception as e:
                print(f"Error retrieving {link['url']}: {e}")
        if dfs:
            all_dfs[stat] = pd.concat(dfs, ignore_index=True)
        else:
            print(f"No data retrieved for category {stat}")
    return all_dfs

stats = ["summary","defense","passing","gca","possession","passing_types","keeper"]
player_links = get_player_links(years, standard_url)

player_links = player_links[:2]

all_match_logs = get_match_logs(years, player_links, stats)


# At the top of your main(), define project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(project_root, "data", "raw")
os.makedirs(data_path, exist_ok=True)

# Save each category separately
for stat, df in all_match_logs.items():
    output_path = os.path.join(data_path, f"player_match_logs_{stat}.csv")
    df.to_csv(output_path, index=False)
    print(f"Saved {stat} match logs to {output_path}")

