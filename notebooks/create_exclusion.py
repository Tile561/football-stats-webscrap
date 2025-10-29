from config import leagues, years, categories
import json
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

def should_exclude(league_name, year, df_name):
    df_name_lower = df_name.lower()

    # ✅ Allow if df_name contains standard or shooting
    if "standard" in df_name_lower or "shooting" in df_name_lower:
        return False

    # ✅ Allow keeper but  EXCLUDE keeperadv
    if "keeper" in df_name_lower and "adv" not in df_name_lower:
        return False

    # Extract start year from "2024-2025" → 2024
    year_start = int(year.split("-")[0])

    league = league_name.lower()

    # Drop based on drop rules
    if league in drop_rules and year_start in drop_rules[league]:
        return True

    return False


def jobs_player_create(leagues, years, categories):
    jobs = []

    for league in leagues:
        for year in years:
            for category, tables in categories.items():
                for name, table_id in tables:

                    # ✅ exclude early years + non standard/shooting/keeper
                    if should_exclude(league["name"], year, name):
                        continue

                    league_url = f"https://fbref.com/en/comps/{league['id']}/{year}/{category}/{year}-{league['name'].replace(' ', '-')}-Stats"
                    
                    jobs.append({
                        "league_name": league["name"],
                        "league_url": league_url,
                        "year": year,
                        "category": category,
                        "df_name": name,
                        "table_id": table_id
                    })

    return jobs

jobs = jobs_player_create(leagues, years, categories)

with open("jobs.json", "w") as f:
    json.dump(jobs, f, indent=4)
