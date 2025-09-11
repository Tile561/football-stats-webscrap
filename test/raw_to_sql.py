import pandas as pd
from sqlalchemy import create_engine, inspect, text
import os

username = os.environ.get("MYSQL_USER")
password = os.environ.get("MYSQL_PASS")
host = 'localhost'  
port = '3306'       
database = 'football_db'

engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')


csv_folder = "csv_exports"

for file in os.listdir(csv_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(csv_folder, file)
        table_name = "raw_" + os.path.splitext(file)[0]  

        df = pd.read_csv(file_path, low_memory=False)

        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Added '{file}' as table '{table_name}'")

print("All CSVs added to MySQL successfully!")

