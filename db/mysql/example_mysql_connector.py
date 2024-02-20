import pandas as pd
import os
import mysql.connector
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

sql = "SELECT * FROM users;"

users = pd.read_sql(sql, mysql_conn_engine)

print(users.head())

# Configure database connection parameters
mysql_config = {
    "host": mysql_host,
    "port": mysql_port,
    "user": mysql_user,
    "password": mysql_password,
    "database": mysql_database,
}

# Establish a database connection
conn = mysql.connector.connect(**mysql_config)

try:
    with conn.cursor() as cursor:
        # Define the SQL query
        query = "SELECT * FROM users;"
        # Execute the SQL query
        cursor.execute(query)
        # Fetch all rows from the last executed query
        results = cursor.fetchall()
        # Iterate through the result and print each row
        for row in results:
            print(row)
except mysql.connector.Error as error:
    print(f"Failed to fetch rows: {error}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed")
