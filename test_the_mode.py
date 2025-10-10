def next_yr():
    today = datetime.date.today()
    year = today.year
    if today.month < 7:
        start_year = year - 1
        end_year = year
    else:
        start_year = year
        end_year = year + 1
    return f"{start_year}-{end_year}"

mode = "historical" 
if mode == "historical":
    years = historical_years
elif mode == "incremental":
    years = [next_yr()]   
