import hashlib


def make_id(row):
    unique_str = f"{row['Player']}_{row['Nation']}_{row['Born']}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def table_make_id(row):
    unique_str = f"{row['Squad']}_{row['league_name']}_team"
    return hashlib.md5(unique_str.encode()).hexdigest()