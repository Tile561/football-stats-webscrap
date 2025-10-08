import os
import json
import pandas as pd
import boto3
from scrappers import scrape_player_stats
from config import *
# TEST WHEN YOU GET HOME
#------------------------------------------------------------------------------------------------------------------------
sqs = boto3.client("sqs", region_name="us-east-1")
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/fbref-scrape-queue"

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
    df = scrape_player_stats(
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
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print("Deleted job from queue.")

if __name__ == "__main__":
    main()
