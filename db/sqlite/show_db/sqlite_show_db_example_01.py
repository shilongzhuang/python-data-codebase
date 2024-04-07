import sqlite3
import os
import pandas as pd

sqlite_default_db_name = 'default'
sqlite_db_extension = '.db'
sqlite_default_db_file = sqlite_default_db_name + sqlite_db_extension
print(sqlite_default_db_file)

conn = sqlite3.connect(sqlite_default_db_file)
sqlite_show_database_query = 'PRAGMA database_list;'

cursor = conn.cursor()
cursor.execute(sqlite_show_database_query)

result = cursor.fetchall()
description = cursor.description
columns = [row[0] for row in description]
print(result, description)
df = pd.DataFrame(result, columns=columns)
print(df)

file_name = df['file'][0]
print(file_name)
print(file_name.split('/')[-1])
os.remove('default.db')
cursor.close()
conn.close()
