import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import warnings
import time
from pipeline.url_builder import create_links, make_matchlog_links
from config import HEADERS, BASE

def scrape_player_stats(name, url, table_id):
    warnings.filterwarnings("ignore")
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    }

    response = requests.get(url, headers=headers,verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    print(response.status_code, url)

    table = soup.find("table", id=table_id) 


    if not table:
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            if table_id in comment:
                print(f"Found {table_id} in comment for {name}")
                comment_soup = BeautifulSoup(comment, 'html.parser')
                table = comment_soup.find('table', id=table_id)
                if table:
                    break

    if not table:
        print(f"Table not found for {name}")
        print(table_id)
        return None


    try:
        df = pd.read_html(str(table), header=1)[0]
    except Exception as e:
        print(f"Error parsing table for {name}: {e}")
        return None

    df['League'] = name

    return df


def scrape_match_data(league_name, url, table_id=None):
    warnings.filterwarnings("ignore")

    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers, verify= False)
    soup = BeautifulSoup(response.content, 'html.parser')
    print(response.status_code, url)

    table = soup.find("table", id=table_id) if table_id else soup.find("table")
    if not table:
        print(f"Table not found for {league_name}")
        return None

    df = pd.read_html(str(table))[0]
    df['League'] = league_name
    return df


def get_player_links(years, leagues):
    links = create_links(years, leagues)
    player_links = []

    for link in links:
        res = requests.get(link, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")

        table_id = "stats_standard"
        table = soup.find("table", id=table_id)

        if not table:
            for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
                if table_id in comment:
                    print(f"found {table_id} for {link}")
                    comment_soup = BeautifulSoup(comment, "html.parser")
                    table = comment_soup.find("table", id=table_id)
                if table:
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
                df_list = combine_data([link["year"]], [link], table_id="matchlogs_all")  # may return a list
                if df_list:  
                    # flatten if df_list is already a list of DataFrames
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