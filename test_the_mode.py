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


def get_player_links(years, leagues, retries=3):
    links = create_links(years, leagues)
    headers = get_headers()
    player_data = []

    for link in links:
        attempt = 0
        while attempt <= retries:
            try:
                response = session.get(link, headers=headers, timeout=15)
                print(response.status_code, link)
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {link}: {e}")
                attempt += 1
                time.sleep(5)
                continue

            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", random.randint(30, 60)))
                print(f" Rate limited! Waiting {wait_time}s before retrying...")
                time.sleep(wait_time)
                attempt += 1
                continue
            elif response.status_code in [403, 500]:
                print(f"Skipping {link} due to HTTP {response.status_code}")
                break  

            # If successful, parse the page
            soup = BeautifulSoup(response.text, "html.parser")
            table_id = "stats_standard"
            table = soup.find("table", id=table_id)

            if not table:
                for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                    if table_id in comment:
                        comment_soup = BeautifulSoup(comment, "html.parser")
                        table = comment_soup.find("table", id=table_id)
                        if table:
                            print(f"Found {table_id} in comment for {link}")
                            break

            if table:
                match = re.search(r'/comps/\d+/(\d{4}-\d{4})/stats/\1-(.*?)-Stats', link)
                year = match.group(1) if match else "Unknown"
                league_name = match.group(2).replace("-", " ") if match else "Unknown"

                player_links = [BASE + a["href"] for a in table.select("td[data-stat='player'] a")]
                print(f"Found {len(player_links)} players for {league_name} {year}")

                for player_link in player_links:
                    player_data.append({
                        "year": year,
                        "league": league_name,
                        "link": player_link
                    })
            break  

        time.sleep(5) 
    return player_data

