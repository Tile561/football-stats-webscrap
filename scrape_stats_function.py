import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import warnings
import time
pd.set_option('display.max_columns', None)


def scrape_player_stats(name, url, table_id):
    warnings.filterwarnings("ignore")
    headers = {'User-Agent': 'Mozilla/5.0'}

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

def scrape_match_stats(name, url, table_id):
    warnings.filterwarnings("ignore")
    headers = {'User-Agent': 'Mozilla/5.0'}

    response = requests.get(url, headers=headers, verify=False)
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


def scrape_league_data(league_name, url, table_id=None):
    #print(f"Scraping {league_name}")
    warnings.filterwarnings("ignore")

    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers, verify= False)
    soup = BeautifulSoup(response.content, 'html.parser')
    print(response.status_code, url)

    table = soup.find("table", id=table_id) if table_id else soup.find("table")
    if not table:
        print(f" Table not found for {league_name}")
        return None

    df = pd.read_html(str(table))[0]
    df['League'] = league_name

    return df


