import os 

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in os.sys.path:
    os.sys.path.append(project_root)

    
import boto3
import datetime
import json
from scrappers.scrappers import get_player_links, make_matchlog_links, create_matchlog_sqs_message
from config import years as historical_years, standard_url

sqs = boto3.client('sqs')
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


def fargate_handler(event, context):
    mode = "historical"
    if mode == "historical":
        years = historical_years
    else:
        years = [next_yr()]
    player_links = get_player_links(years, standard_url)
    matchlog_jobs = create_matchlog_sqs_message(make_matchlog_links(player_links))

    matchlog_queue = "https://sqs.us-east-2.amazonaws.com/117446093992/football_matchlog_queue"

    for job in matchlog_jobs:
        sqs.send_message(
            QueueUrl = matchlog_queue,
            MessageBody = json.dumps(job)
            (job, ensure_ascii=False)
        )

    return {"Status": "ok", "job_sent":  len(matchlog_jobs)}