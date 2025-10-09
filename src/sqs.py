import boto3
import json
from scrappers.scrappers import scrape_player_stats

from pprint import pprint

sqs = boto3.client(
    "sqs",
    region_name="us-east-1",
    endpoint_url="https://localhost.localstack.cloud:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    aws_session_token="test"  # optional
)


queue_url = "https://localhost.localstack.cloud:4566/117446093992/fbref"

print(f"SQS Queue URL: {queue_url}")

leagues = [
    {"name": "Bundesliga", "id": 20},
    {"name": "Premier League", "id": 9},
    {"name": "La Liga", "id": 12},
    {"name": "Serie A", "id": 11},
    {"name": "Ligue 1", "id": 13},
    {"name": "Eredivisie", "id": 23},
    {"name": "Primeira Liga", "id": 32},
    {"name": "Championship", "id": 10},
    {"name": "Scottish Premiership", "id": 40},
    {"name": "Belgian Pro League", "id": 37},     # Belgium
    {"name": "Danish Superliga", "id": 50},       # Denmark
    {"name": "Swiss Super League", "id": 57},     # Switzerland
    {"name": "Austrian Bundesliga", "id": 56},    # Austria
    {"name": "Turkish Süper Lig", "id": 26},      # Turkey
]
years = ["2024-2025","2023-2024","2022-2023","2021-2022","2020-2021","2019-2020","2018-2019","2017-2018","2016-2017","2015-2016","2014-2015","2013-2014","2012-2013","2011-2012","2010-2011"]

categories = {
    "gca": [
        ("player_gca_df", "stats_gca"),
        ("team_gca_df", "stats_squads_gca_for"),
        ("team_gca_against_df", "stats_squads_gca_against"),
    ],
    "keepersadv": [
        ("player_keepersadv_df", "stats_keeper_adv"),
        ("team_keepersadv_df", "stats_squads_keeper_adv_for"),
        ("team_keepersadv_against_df", "stats_squads_keeper_adv_against"),
    ],
    "shooting": [
        ("player_shooting_df", "stats_shooting"),
        ("team_shooting_df", "stats_squads_shooting_for"),
        ("team_shooting_against_df", "stats_squads_shooting_against"),
    ],
    "passing": [
        ("player_passing_df", "stats_passing"),
        ("team_passing_df", "stats_squads_passing_for"),
        ("team_passing_against_df", "stats_squads_passing_against"),
    ],
    "passing_types": [
        ("player_passing_type_df", "stats_passing_types"),
        ("team_passing_type_df", "stats_squads_passing_types_for"),
        ("team_passing_type_against_df", "stats_squads_passing_types_against"),
    ],
    "defense": [
        ("player_defense_df", "stats_defense"),
        ("team_defense_df", "stats_squads_defense_for"),
        ("team_defense_against_df", "stats_squads_defense_against"),
    ],
    "possession": [
        ("player_possession_df", "stats_possession"),
        ("team_possession_df", "stats_squads_possession_for"),
        ("team_possession_against_df", "stats_squads_possession_against"),
    ],
    "stats": [
        ("stats_player_standard_df", "stats_standard"),
        ("stats_squad_standard_df", "stats_squads_standard_for"),
        ("stats_squad_standard_against_df", "stats_squads_standard_against"),
    ],
    "keepers": [
        ("stats_keeper_df", "stats_keeper"),
        ("stats_squad_keeper_df", "stats_squads_keeper_for"),
        ("stats_squad_keeper_against_df", "stats_squads_keeper_against"),
    ],
}

categories_match = {
    "league": [
            ("league_finish_df", None),
        ],
        "schedule": [
            ("schedule_df", None),
        ]
}


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
                #print(job)
                #print(json.dumps(job, indent=4,ensure_ascii=False))


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
                #print(json.dumps(job, indent=4,ensure_ascii=False))
                #pprint(job)


for job in jobs[:5]:  # sending only first 5 jobs for testing
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(job, ensure_ascii=False)
    )

messages = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,
    WaitTimeSeconds=2
)

if "Messages" in messages:
    for msg in messages["Messages"]:
        job = json.loads(msg["Body"])
        pprint(job)

        # Optional: scrape data locally
        df = scrape_player_stats(job["df_name"], job["league_url"], job["table_id"])
        # print(df.head())

        # Delete message after processing
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg["ReceiptHandle"]
        )   


'''
# Print first 5 jobs for quick inspection
print("\n--- Sample Jobs ---\n")
for job in jobs[:5]:
    df = scrape_player_stats(
        job["df_name"], 
        job["league_url"], 
        job["table_id"]
    )
    print(f"\n--- {job['league_name']} {job['year']} ---")
    print(df.head())
    

# Optional: test scraping function on the first job
# from scrappers.scrappers import scrape_player_stats
df = scrape_player_stats(
    jobs[0]["df_name"], 
    jobs[0]["league_url"], 
    jobs[0]["table_id"]
)

print(df.head())
'''
