import sqlite3
import os
import pycountry
import pandas as pd

# Specify the database and table name
database_name = 'mydb.db'
table_name = 'countries'

# Connect to SQLite database
conn = sqlite3.connect(database_name)
cursor = conn.cursor()

# Drop table if it exists
sql = """DROP TABLE IF EXISTS {};""".format(table_name)
cursor.execute(sql)

# Create table if it doesn't exist
sql = """
CREATE TABLE IF NOT EXISTS {} (
                    alpha_2_code TEXT PRIMARY KEY,
                    name TEXT,
                    alpha_3_code TEXT,
                    numeric TEXT
                );
""".format(table_name)
cursor.execute(sql)

# Fetch country data from pycountry and insert into the database
try:
    insert_sql_string = "INSERT OR IGNORE INTO {} (alpha_2_code, name, alpha_3_code, numeric) VALUES (?, ?, ?, ?)".format(table_name)
    for country in pycountry.countries:
        alpha_2_code = country.alpha_2
        name = country.name
        alpha_3_code = country.alpha_3
        numeric = country.numeric
        cursor.execute(insert_sql_string, (alpha_2_code, name, alpha_3_code, numeric))
    # Commit changes and close connection
    conn.commit()
    print("Country data loaded successfully.")

    sql = "SELECT * FROM {}".format(table_name)
    cursor.execute(sql)
    result = cursor.fetchall()
    description = cursor.description
    columns = [row[0] for row in description]
    df = pd.DataFrame(result, columns=columns)
    print(result, columns)
    print(df.info())
    """
    Data columns (total 4 columns):
     #   Column        Non-Null Count  Dtype 
    ---  ------        --------------  ----- 
     0   alpha_2_code  249 non-null    object
     1   name          249 non-null    object
     2   alpha_3_code  249 non-null    object
     3   numeric       249 non-null    object
    """

    print(df.head())
    """
      alpha_2_code           name alpha_3_code numeric
    0           AW          Aruba          ABW     533
    1           AF    Afghanistan          AFG     004
    2           AO         Angola          AGO     024
    3           AI       Anguilla          AIA     660
    4           AX  Åland Islands          ALA     248
    """
except sqlite3.Error as e:
    print("SQLite error:", e)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if database_name:
        os.remove(database_name)
