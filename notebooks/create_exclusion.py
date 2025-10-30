from config import leagues, years, categories
import json
# Drop rules for player and team DataFrames
drop_rules = {
    'eredivisie': list(range(2010, 2018)),
    'championship': list(range(2010, 2018)),
    'primeira liga': list(range(2010, 2018)),
    'bundesliga': list(range(2010, 2017)),
    'premier league': list(range(2010, 2017)),
    'la liga': list(range(2010, 2017)),
    'serie a': list(range(2010, 2017)),
    'ligue 1': list(range(2010, 2017)),
    'scottish premiership': list(range(2010, 2026)),
    'belgian pro league': list(range(2010, 2017)),
    'danish superliga': list(range(2010, 2026)),
    'swiss super league': list(range(2010, 2026)),
    'austrian bundesliga': list(range(2010, 2026)),
    'turkish süper lig': list(range(2010, 2026))

}

against_drop_rules = {
    'eredivisie': list(range(2010, 2017)),
    'championship': list(range(2010, 2016)),
    'primeira liga': list(range(2010, 2016)),
    'bundesliga': list(range(2010, 2016)),
    'premier league': list(range(2010, 2016)),
    'la liga': list(range(2010, 2015)),
    'serie a': list(range(2010, 2015)),
    'ligue 1': list(range(2010, 2015)),
    'scottish premiership': list(range(2010, 2016)),
    'belgian pro league': list(range(2010, 2015)),
    'danish superliga': list(range(2010, 2015)),
    'swiss super league': list(range(2010, 2015)),
    'austrian bundesliga': list(range(2010, 2016)),
    'turkish süper lig': list(range(2010, 2015))
}

def should_exclude(league_name, year, df_name):
    df_name_lower = df_name.lower()
    year_start = int(year.split("-")[0])
    league = league_name.lower()

    # --- 1️⃣ Handle "against" dataframes first ---
    if "against" in df_name_lower:
        if any( kw in df_name_lower for kw in ["standard", "shooting", "keeper"] ):
            return False
        if league in against_drop_rules and year_start in against_drop_rules[league]:
            return True
        else:
            return False

    # --- 2️⃣ Allow certain dataframe types regardless of rules ---
    if "standard" in df_name_lower or "shooting" in df_name_lower:
        return False

    if "keeper" in df_name_lower and "adv" not in df_name_lower:
        return False

    # --- 3️⃣ Apply normal drop rules ---
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
