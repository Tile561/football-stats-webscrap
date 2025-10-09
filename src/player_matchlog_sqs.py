print("✅ Script started...")
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import time
from config import HEADERS, BASE, player_matchlog_categories, years, standard_url
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import re
from scrappers.scrappers import scrape_all_stats

import logging
logging.basicConfig(level=logging.DEBUG)


def create_session():
    retry_strategy = Retry(
        total=3,  # Total retry attempts
        backoff_factor=1, 
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/15.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.5993.70 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": "https://google.com",
    }

session = create_session()

def create_links(years, leagues):
    links = []
    for league in leagues:
        for year in years:
            url = league["url"].format(year=year)
            links.append(url)
    return links

def get_player_links(years, leagues, retries = 3):
    links = create_links(years, leagues)
    headers = get_headers()
    player_links = []
    player_data = []


    for link in links:
        try:
            response = session.get(link, headers=headers, timeout=15)
            print(response.status_code, link)
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {link}: {e}")
            continue

        if response.status_code == 429:
            wait_time = random.randint(30, 60)
            print(f"⚠️ Rate limited! Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            continue
        elif response.status_code in [403, 500]:
            print(f"⚠️ Skipping {link} due to HTTP {response.status_code}")
            continue

        match = re.search(r'/comps/\d+/(\d{4}-\d{4})/stats/\1-(.*?)-Stats', link)
        if match:
            year = match.group(1)
            league_name = match.group(2).replace("-", " ")
        else:
            year = "Unknown"
            league_name = "Unknown"

        
        if response.status_code in [403, 429]:
            if retries > 0:
                retry_after = int(response.headers.get("Retry-After", 10))
                print(f"Blocked or rate-limited ({response.status_code}) at {link}")
                time.sleep(retry_after + random.uniform(1,3))
                return get_player_links(years, leagues, retries=retries - 1)
            else:
                print(f"Max retries exceeded for this {link}")
                return pd.DataFrame()
        
        soup = BeautifulSoup(response.text, "html.parser")
        table_id = "stats_standard"
        table = soup.find("table", id=table_id)

        # Check if table is hidden inside comments
        if not table:
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if table_id in comment:
                    comment_soup = BeautifulSoup(comment, 'html.parser')
                    table = comment_soup.find('table', id=table_id)
                    if table:
                        print(f"Found {table_id} in comment for {link}")
                        break
        
        if table: 
            player_links = [BASE + a["href"] for a in table.select("td[data-stat='player'] a")]
            print(f"Found {len(player_links)} players for {league_name} {year}")


            for player_link in player_links:
                player_data.append({
                    "year": year,
                    "league": league_name,
                    "link":player_link
                })

        time.sleep(5)
    return player_data

def make_matchlog_links(player_urls):
    links = []

    for player in player_urls:
        url = player["link"]
        year = player["year"]
        #league  = player["league"]


        parts = url.strip("/").split("/")

        if len(parts) < 2:
            continue

        player_id = parts[-2]
        player_slug = parts[-1]

        for stat in player_matchlog_categories:
            matchlog_url = f"https://fbref.com/en/players/{player_id}/matchlogs/{year}/{stat}/{player_slug}-Match-Logs"
            links.append({
                "name": f"{player_slug} - {stat}",
                "url": matchlog_url
            })
    unique_links = {link["url"]: link for link in links}.values()
    return unique_links

def create_matchlog_sqs_message(links):
    jobs = []
    for link in links:
        job = {
            "player_url": link["url"],
            "player_name":link["name"],
            "table_id": "matchlogs_all"
        }
        jobs.append(job)
    return jobs


player_links = get_player_links(years, standard_url)
links = make_matchlog_links(player_links)
jobs = create_matchlog_sqs_message(links)

dfs = []
for job in jobs:
    print(f"Scraping {job['player_name']} from {job['player_url']}")
    df = scrape_all_stats(job["player_name"], job["player_url"], job["table_id"])
    dfs.append(df)





    


    
    

