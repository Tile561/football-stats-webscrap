import pandas as pd

def clean_text_columns(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
    return df

def extract_league_and_season(df):
    if 'League' not in df.columns:
        return df

    df[['league_name', 'season']] = df['League'].str.rsplit(' ', n=1, expand=True)
    df['season_start'] = pd.to_numeric(df['season'].str[:4], errors='coerce')
    df = df.dropna(subset=['season_start'])
    df['season_start'] = df['season_start'].astype(int)
    return df

def auto_int_convert(df):
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            if df[col].dropna().apply(float.is_integer).all():
                df[col] = df[col].astype('Int64')
    return df
