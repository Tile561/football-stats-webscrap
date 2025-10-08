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
        backoff_factor=1,  # Exponential backoff: 1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_session()

def scrape_player_stats(name, url, table_id):
    """Scrape player statistics table for a given league and season."""
    warnings.filterwarnings("ignore")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        response = session.get(url, headers=headers, timeout=15)
        print(response.status_code, url)
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return None

    if response.status_code in [403, 429]:
        print(f"Blocked or rate-limited ({response.status_code}) at {url}")
        time.sleep(random.uniform(5, 10))  # backoff with jitter
        return None

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
        return None

    # Parse table into DataFrame
    try:
        df = pd.read_html(str(table), header=1)[0]
        df['League'] = name
        return df
    except Exception as e:
        print(f"Error parsing table for {name}: {e}")
        return None



def scrape_match_data(league_name, url, table_id=None):
    warnings.filterwarnings("ignore")
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        print(response.status_code, url)
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return None

    if response.status_code in [403, 429]:
        print(f"Blocked or rate-limited ({response.status_code}) at {url}")
        time.sleep(5)
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", id=table_id) if table_id else soup.find("table")
    if not table:
        print(f"Table not found for {league_name}")
        return None

    try:
        df = pd.read_html(str(table))[0]
        df['League'] = league_name
        return df
    except Exception as e:
        print(f"Error parsing match data for {league_name}: {e}")
        return None



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
    matchlog_links = make_matchlog_links(player_links, years, stats)
    
    all_dfs = {}
    for stat, links in matchlog_links.items():
        dfs = []
        for link in links:
            try:
                df_list = combine_data(years, [link], table_id="matchlogs_all")  # may return a list
                if df_list:  
                    if isinstance(df_list, list):
                        dfs.extend(df_list)
                    else:
                        dfs.append(df_list)
            except Exception as e:
                print(f"Error retrieving {link['url']}: {e}")
        if dfs:
            all_dfs[stat] = pd.concat(dfs, ignore_index=True)
        else:
            print(f"No data retrieved for category {stat}")
    return all_dfs

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