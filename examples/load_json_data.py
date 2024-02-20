import pandas as pd
import requests
import os
import time
from sqlalchemy import create_engine

mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_port = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")

# Build the MySQL connection string
mysql_conn_uri = f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}"

# Create an SQLAlchemy engine
mysql_conn_engine = create_engine(mysql_conn_uri)

url = "https://dummyjson.com/users"
response = requests.get(url)
status_code = response.status_code
print(status_code)
json_data = response.json()
df = pd.DataFrame(json_data['users'])
columns = ['id', 'firstName', 'lastName', 'age', 'gender', 'email', 'username', 'birthDate']
data = df[columns].rename(columns={'firstName': 'first_name', 'lastName': 'last_name', 'birthDate': 'dob'})
print(data.head())
# Measure start time
start_time = time.time()
data.to_sql('users', con=mysql_conn_engine, if_exists='replace', index=False)
# Measure end time
end_time = time.time()
# Calculate duration
duration = end_time - start_time
print(f'Total time taken for file operations: {duration} seconds')
