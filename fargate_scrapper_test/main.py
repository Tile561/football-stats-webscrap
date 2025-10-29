import boto3
import datetime
from io import StringIO
import json
from scrappers.scrappers import scrape_all_stats


player_queue = "https://sqs.us-east-2.amazonaws.com/117446093992/football_player_queue"
#team_queue = "https://sqs.us-east-2.amazonaws.com/117446093992/football_team_queue"
#matchlog_queue = "https://sqs.us-east-2.amazonaws.com/117446093992/football_matchlog_queue"

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def upload_to_s3(df, filename, bucket_name= "raw-football-data"):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=filename, Body=csv_buffer.getvalue())


    

    
def process_sqs_queue():
    while True:
        messages = sqs.receive_message(
            QueueUrl = player_queue,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 10,
            VisibilityTimeout = 30
        )
        if "Messages" not in messages:
            continue

    
        for msg in messages["Messages"]:
            try:
                job = json.loads(msg["Body"])

                df = scrape_all_stats(job["df_name"],job["league_url"],job["table_id"])

                filename = f"{job.get('player_name', job.get('df_name'))}_{job['table_id']}.csv"
                upload_to_s3(df, filename)

                sqs.delete_message(
                    QueueUrl=player_queue,
                    ReceiptHandle=msg["ReceiptHandle"]
                )
            except Exception as e:
                print(f"Error processing message {msg['MessageId']}: {e}")
