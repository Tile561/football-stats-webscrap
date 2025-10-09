import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import warnings
import time
from pipeline.url_builder import create_links, make_matchlog_links
from config import HEADERS, BASE
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

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



def get_player_links(years, leagues):
    links = create_links(years, leagues)
    player_links = []

    for link in links:
        try:
            res = requests.get(link, headers=HEADERS)
            print(res.status_code, link)
        except requests.exceptions.RequestException as e:
            print(f"Error feching {link}: {e}")
            continue


        soup = BeautifulSoup(res.text, "html.parser")
        table_id = "stats_standard"
        table = soup.find("table", id=table_id)

        if not table:
            for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
                if table_id in comment:
                    comment_soup = BeautifulSoup(comment, "html.parser")
                    table = comment_soup.find("table", id=table_id)
                if table:
                    print(f"found {table_id} for {link}")
                    break

        if table:
            links_found = [BASE + a["href"] for a in table.select("td[data-stat='player'] a")]
            player_links.extend(links_found)

        time.sleep(5)  
    return player_links

def get_match_logs(years, player_links, stats):
    all_dfs = {}

    # Loop over years one by one (newest to oldest, if list is ordered that way)
    for year in years:
        print(f"Scraping year: {year}")

        # Build match log links only for this year
        matchlog_links = make_matchlog_links(player_links, [year], stats)

        for stat, links in matchlog_links.items():
            dfs = []
            for link in links:
                try:
                    df_list = combine_data([year], [link], table_id="matchlogs_all")
                    if df_list:
                        if isinstance(df_list, list):
                            dfs.extend(df_list)
                        else:
                            dfs.append(df_list)
                except Exception as e:
                    print(f"Error retrieving {link['url']}: {e}")

            if dfs:
                if stat not in all_dfs:
                    all_dfs[stat] = []
                all_dfs[stat].append(pd.concat(dfs, ignore_index=True))
            else:
                print(f"No data retrieved for {stat} in {year}")

    # Concatenate per stat across all years
    final_dfs = {stat: pd.concat(dfs, ignore_index=True) for stat, dfs in all_dfs.items()}
    return final_dfs


def combine_data(years,leagueinfo, table_id):
    leagues = []
    for year in years:
        for league in leagueinfo:
            leagues.append({
                "name": f"{league['name']} {year}",
                "url": league["url"].format(year=year),
            })

    league_dfs = {}
    combined_list = []
    try:
        for league in leagues:
            df = scrape_player_stats(league["name"], league["url"], table_id=table_id)
            if df is not None:
                league_dfs[league["name"]] = df
                combined_list.append(df)
            else:
                print(f"Failed for {league['name']} at {league['url']}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Scraping interrupted")
    return combined_list


def combine_match_data(years,leagueinfo,table_id):
    leagues = []
    for year in years:
        for league in leagueinfo:
            leagues.append({
                "name": f"{league['name']} {year}",
                "url": league["url"].format(year=year),
            })

    league_dfs = {}
    combined_list = []
    try:
        for league in leagues:
            df = scrape_match_data(league["name"], league["url"], table_id=table_id)
            if df is not None:
                league_dfs[league["name"]] = df
                combined_list.append(df)
            else:
                print(f"Failed for {league['name']} at {league['url']}")
            time.sleep(5) 
    except KeyboardInterrupt:
        print("Scraping interrupted")
    return combined_list

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
