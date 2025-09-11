import pandas as pd
import hashlib
from config import team_drop_rules, against_drop_rules
from config import exclude_team_tables, exclude_tables_against, against_tables
from pipeline.common_cleaning import auto_int_convert, extract_league_and_season, clean_text_columns

def apply_team_drop_rules(df, df_name):
    if 'league_name' not in df.columns or 'season_start' not in df.columns:
        return df
    rules = against_drop_rules if df_name in exclude_tables_against else team_drop_rules  

    mask = df.apply(
        lambda row: row['season_start'] in rules.get(row['league_name'], []),
        axis=1
    )
    return df[~mask]

def table_make_id(row):
    unique_str = f"{row['Squad']}_{row['league_name']}_team"
    return hashlib.md5(unique_str.encode()).hexdigest()

def split_league_name(df,df_name):
    if df_name in against_tables:
        split = df['Squad'].str.split(' ', n=1, expand=True)
        if split.shape[1] == 2:
            df['Vs Squad'], df['Squad'] = split[0], split[1]
        df['Vs Squad'] = df['Vs Squad'].astype(str).str.strip().str.lower()
    return df

def process_team_df(df, df_name, skip_df=False):
    df = auto_int_convert(df)
    df = extract_league_and_season(df)
    df = split_league_name(df,df_name)
    df = clean_text_columns(df, ['Squad','League','league_name'])

    if not skip_df:
        df = apply_team_drop_rules(df, df_name=df_name)
    return df
