import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import warnings
import time
from pipeline.url_builder import create_links, make_matchlog_links
from config import HEADERS, BASE, categories_match,player_matchlog_categories
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import re

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

session = create_session()

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

def scrape_player_stats(name, url, table_id, retries =3):
    """Scrape player statistics table for a given league and season."""
    warnings.filterwarnings("ignore")
    headers = get_headers()
    try:
        response = session.get(url, headers=headers, timeout=15)
        print(response.status_code, url)
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return pd.DataFrame()

    if response.status_code in [403, 429]:
        if retries > 0:
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Blocked or rate-limited ({response.status_code}) at {url}")
            time.sleep(retry_after + random.uniform(1,3))
            return scrape_player_stats(name, url, table_id, retries - 1)
        else:
            print(f"Max retries exceeded for this {url}")
            return pd.DataFrame()
        
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", id=table_id)

    # Check if table is hidden inside comments
    if not table:
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            if table_id in comment:
                comment_soup = BeautifulSoup(comment, 'html.parser')
                table = comment_soup.find('table', id=table_id)
                if table:
                    print(f"Found {table_id} in comment for {name}")
                    break

    if not table:
        print(f"Table not found for {name} ({table_id})")
        return pd.DataFrame()

    # Parse table into DataFrame
    try:
        df = pd.read_html(str(table), header=1)[0]
        df['League'] = name
        time.sleep(5)
        return df
    except Exception as e:
        print(f"Error parsing table for {name}: {e}")
        return pd.DataFrame()


def scrape_match_data(league_name, url, table_id=None, retries=3):
    warnings.filterwarnings("ignore")
    headers = get_headers()
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        print(response.status_code, url)
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return pd.DataFrame()

    if response.status_code in [403, 429]:
        if retries > 0:
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Blocked or rate-limited ({response.status_code}) at {url}")
            time.sleep(retry_after + random.uniform(1,3))
            return scrape_match_data(league_name, url, table_id=None, retries=retries - 1)
        else:
            print(f"Max retries exceeded for this {url}")
            return pd.DataFrame()

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", id=table_id) if table_id else soup.find("table")
    if not table:
        print(f"Table not found for {league_name}")
        return pd.DataFrame()

    try:
        df = pd.read_html(str(table))[0]
        df['League'] = league_name
        time.sleep(5)
        return df
    except Exception as e:
        print(f"Error parsing match data for {league_name}: {e}")
        return pd.DataFrame()





def scrape_all_stats(name, url, table_id, retries =3):
    warnings.filterwarnings("ignore")
    headers = get_headers()
    try:
        response = session.get(url, headers=headers, timeout=15)
        print(response.status_code, url)
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return pd.DataFrame()

    if response.status_code in [403, 429]:
        if retries > 0:
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Blocked or rate-limited ({response.status_code}) at {url}")
            time.sleep(retry_after + random.uniform(1,3))
            return scrape_player_stats(name, url, table_id, retries - 1)
        else:
            print(f"Max retries exceeded for this {url}")
            return pd.DataFrame()
        
    soup = BeautifulSoup(response.content, 'html.parser')


    if table_id is None:
        table = soup.find("table")
    else:
        table = soup.find ("table", id = table_id)
    
    #table = soup.find("table", id=table_id)

    # Check if table is hidden inside comments
    if not table:
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            #Check this when home
            if table_id and table_id in comment: 
                comment_soup = BeautifulSoup(comment, 'html.parser')
                table = comment_soup.find('table', id=table_id)
                if table:
                    print(f"Found {table_id} in comment for {name}")
                    break

    if not table:
        print(f"Table not found for {name} ({table_id})")
        return pd.DataFrame()

    # Parse table into DataFrame
    try:
        df = pd.read_html(str(table), header=1)[0]
        df['League'] = name
        time.sleep(5)
        return df
    except Exception as e:
        print(f"Error parsing table for {name}: {e}")
        return pd.DataFrame()

def jobs_player_create(leagues,years, categories):
    jobs = []
    for league in leagues:
        for year in years:
            for category, tables in categories.items():
                for name, table_id in tables:
                    league_url = f"https://fbref.com/en/comps/{league['id']}/{year}/{category}/{year}-{league['name'].replace(' ', '-')}-Stats"
                    job = {
                        "league_name": league["name"],
                        "league_url": league_url,
                        "year": year,
                        "category": category,
                        "df_name": name,
                        "table_id": table_id
                    }
                    jobs.append(job)
    return jobs


def jobs_team_create(leagues, years, category_match):
    match_jobs = []
    for league in leagues:
        for year in years:
            for category_match, tables_match in categories_match.items():
                for name_match, table_id_match in tables_match:
                    league_url = f"https://fbref.com/en/comps/{league['id']}/{year}/{category_match}/{year}-{league['name'].replace(' ', '-')}-Stats"
                    job = {
                        "league_name": league["name"],
                        "league_url": league_url,
                        "year": year,
                        "category": category_match,
                        "df_name": name_match,
                        "table_id": table_id_match
                    }
                    match_jobs.append(job) 
    return match_jobs


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
