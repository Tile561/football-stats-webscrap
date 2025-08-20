import pandas as pd
import os

folder = "csv_exports"
dropped_folder = "dropped_csvs"
os.makedirs(dropped_folder, exist_ok=True)

adv_stats_files = [
    'player_defense_df',
    'player_gca_df',
    'player_keepersadv_df',
    'player_passing_df',
    'player_passing_type_df',
    'player_possession_df',
    'team_defense_df',
    'team_defense_against_df',
    'team_gca_df',
    'team_gca_against_df',
    'team_keepersadv_df',
    'team_passing_df',
    'team_passing_against_df',
    'team_passing_type_df',
    'team_passing_type_against_df',
    'team_possession_df',
    'team_possession_against_df'
]

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

for file_name in adv_stats_files:
    path = os.path.join(folder, f"{file_name}.csv")
    if not os.path.exists(path):
        continue
    
    df = pd.read_csv(path)
    if 'League' not in df.columns:
        continue

    df[['league_name', 'season']] = df['League'].str.rsplit(' ', n=1, expand=True)
    df['league_name'] = df['league_name'].str.strip().str.lower()
    df['season_start'] = pd.to_numeric(df['season'].str[:4], errors='coerce')
    df = df.dropna(subset=['season_start'])
    df['season_start'] = df['season_start'].astype(int)

    mask = (df['league_name'].isin(drop_rules.keys())) & \
           df.apply(lambda row: row['season_start'] in drop_rules[row['league_name']], axis=1)
    df_cleaned = df[~mask]

    df_cleaned.to_csv(os.path.join(dropped_folder, f"{file_name}_cleaned.csv"), index=False)