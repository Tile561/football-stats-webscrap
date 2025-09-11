import pandas as pd
from config import drop_rules
from config import exclude_tables
from pipeline.common_cleaning import auto_int_convert, extract_league_and_season, clean_text_columns

def apply_drop_rules(df, drop_rules):
    if 'league_name' not in df.columns or 'season_start' not in df.columns:
        return df

    mask = df.apply(
        lambda row: row['season_start'] in drop_rules.get(row['league_name'], []),
        axis=1
    )
    return df[~mask]

def process_player_df(df, skip_df=False):
    df = auto_int_convert(df)
    df = extract_league_and_season(df)
    df = clean_text_columns(df, ['Player', 'Nation', 'Squad', 'League','league_name','Pos'])

    df = df.dropna(subset=['Player', 'Nation', 'Born'], how='all')
    df['Player'] = df['Player'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

    if not skip_df:
        df = apply_drop_rules(df, drop_rules)
    return df
