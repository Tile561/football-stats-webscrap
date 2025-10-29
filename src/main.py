import pandas as pd
from config import years, leagues, categories, categories_match, standard_url, player_matchlog_categories
from pipeline.url_builder import build_leagueinfo
from scrappers.scrappers import scrape_player_stats, scrape_match_data
from pipeline.url_builder import build_leagueinfo
from scrappers.scrappers import combine_data, combine_match_data
from scrappers.scrappers import get_player_links, get_match_logs
import json 
import boto3
import os


sqs = boto3.client(
    "sqs",
    region_name="us-east-1",
    endpoint_url="https://localhost.localstack.cloud:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    aws_session_token="test"  # optional
)


queue_url = "https://localhost.localstack.cloud:4566/117446093992/fbref"

def main():

    #  Get a message from SQS
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5,
    )

    if "Messages" not in response:
        print("No messages in queue.")
        return

    message = response["Messages"][0]
    receipt_handle = message["ReceiptHandle"]
    job = json.loads(message["Body"])
    print(f"Processing job: {job}")

    # Scrape one dataset
    df = scrape_match_data(
        job["league_name"], 
        job["league_url"], 
        job["table_id"]
    )

    #Save output (locally for testing)
    if df is not None:
        os.makedirs("data/raw", exist_ok=True)
        filename = f"{job['league_name'].replace(' ', '_')}_{job['year']}_{job['df_name']}.csv"
        filepath = os.path.join("data/raw", filename)
        df.to_csv(filepath, index=False)
        print(f"Saved: {filepath}")
    else:
        print("No data scraped.")

    #Delete message from SQS to prevent reprocessing
    #sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    #print("Deleted job from queue.")
    
    player_links = get_player_links(years, standard_url)

    #player_links = player_links[:5]
    #print(player_links)
    all_match_logs = get_match_logs(years, player_links, player_matchlog_categories)

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_root, "data", "raw")
    os.makedirs(data_path, exist_ok=True)

    for stat, df in all_match_logs.items():
        output_path = os.path.join(data_path, f"player_match_logs_{stat}_raw.csv")
        df.to_csv(output_path, index=False)
        print(f"Saved {stat} match logs to {output_path}")



if __name__ == "__main__":
    main()


'''
    
    # Stats categories
    dataframes = {}
    for category, tables in categories.items():
        leagueinfo = build_leagueinfo(leagues, category)
        for name, table_id in tables:
            dfs = combine_data(years, leagueinfo, table_id=table_id)  
            dataframes[name] = dfs  

    # Match-related categories
    dataframes_match = {}
    for category, tables in categories_match.items():
        leagueinfo = build_leagueinfo(leagues, category)
        for name, table_id in tables:
            dfs = combine_match_data(years, leagueinfo, table_id=table_id) 
            dataframes_match[name] = dfs  

    # Save stats categories
    for name, dfs in dataframes.items():
        if isinstance(dfs, list):  
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
            else:
                print("No data found")
                continue
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/raw_{name}.csv", index=False)

    # Save match-related categories
    for name, dfs in dataframes_match.items():
        if isinstance(dfs, list):
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
            else:
                print("No data found")
                continue
        else:
            df = dfs
        os.makedirs("data/raw", exist_ok=True)
        df.to_csv(f"data/raw/raw_{name}.csv", index=False)
'''