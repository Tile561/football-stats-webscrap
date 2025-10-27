import boto3
import datetime
import json
from io import StringIO
from scrappers.scrappers import jobs_player_create, jobs_team_create, scrape_all_stats, get_player_links, make_matchlog_links, create_matchlog_sqs_message
from config import leagues, years as historical_years, categories, categories_match, standard_url

sqs = boto3.client(
    "sqs",
    region_name="us-east-1",
    endpoint_url="https://localhost.localstack.cloud:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    aws_session_token="test"  # optional
)


s3 = boto3.client(
    "s3",
    region_name="us-east-1",
    endpoint_url="s3://test2",  # Remove for AWS
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

def upload_to_s3(df, filename, bucket="fbref-data"):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=filename, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded {filename} to S3")

player_queue = "https://localhost.localstack.cloud:4566/117446093992/player_fbref"
team_queue = "https://localhost.localstack.cloud:4566/117446093992/team_fbref"
matchlog_queue = "https://localhost.localstack.cloud:4566/117446093992/matchlog_fbref"

#print(f"SQS Queue URL: {queue_url}")
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

mode = "incremental" 
if mode == "historical":
    years = historical_years
elif mode == "incremental":
    years = [next_yr()]   

player_jobs = jobs_player_create(leagues, years, categories)
team_jobs = jobs_team_create(leagues, years, categories_match)

player_links = get_player_links(years, standard_url)
links = make_matchlog_links(player_links)
matchlog_jobs = create_matchlog_sqs_message(links)



for job in player_jobs[:5]:  # sending only first 5 jobs for testing
    sqs.send_message(
        QueueUrl=player_queue,
        MessageBody=json.dumps(job, ensure_ascii=False)
    )

for job in team_jobs[:5]:  # sending only first 5 jobs for testing
    sqs.send_message(
        QueueUrl=team_queue,
        MessageBody=json.dumps(job, ensure_ascii=False)
    )

for job in matchlog_jobs[:5]:  # sending only first 5 jobs for testing
    sqs.send_message(
        QueueUrl=matchlog_queue,
        MessageBody=json.dumps(job, ensure_ascii=False)
    )
def procces_queue(queue_url):
    while True:
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2
        )

        if "Messages" in messages:
            for msg in messages["Messages"]:
                job = json.loads(msg["Body"])
                print(job)

                # Optional: scrape data locally
                if "player_name" in job and "player_url" in job:
                    df = scrape_all_stats(job["player_name"], job["player_url"], job["table_id"])
                elif "df_name" in job and "league_url" in job:
                    df = scrape_all_stats(job["df_name"], job["league_url"], job["table_id"])
                
                else:
                    print(f"⚠️ Skipping unknown job format: {job}")
                    continue
                filename = f"{job.get('player_name', job.get('df_name'))}_{job['table_id']}.csv"
                upload_to_s3(df, filename)

                print(df.head())

                # Delete message after processing
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )   

for q in [player_queue, team_queue, matchlog_queue]:
    procces_queue(q)
