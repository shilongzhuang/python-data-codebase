import sqlite3
import os
import pandas as pd

db_name = "library.db"
table_name = "books"

# Connect to the SQLite database
conn = sqlite3.connect(db_name)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Drop a table if it is an existing one
drop_sql_string = """DROP TABLE IF EXISTS {};""".format(table_name)
cursor.execute(drop_sql_string)

# Create a table
create_sql_string = """
    CREATE TABLE IF NOT EXISTS {} 
        (id INTEGER PRIMARY KEY, 
        title TEXT, 
        author TEXT, 
        publication_year INTEGER);
""".format(table_name)
conn.execute(create_sql_string)

# Insert some sample data into the books table
insert_sql_string = "INSERT OR IGNORE INTO {} (id, title, author, publication_year) VALUES (?, ?, ?, ?)".format(
    table_name)
cursor.execute(insert_sql_string, (1, 'Super Pete', 'James Dean', 2020))
cursor.execute(insert_sql_string, (2, 'Big Easter Adventure', 'James Dean', 2014))
cursor.execute(insert_sql_string, (3, 'I Love My White Shoes', 'James Dean', 2010))
conn.commit()

# Read and print all books from the table
select_sql_string = "SELECT * FROM {}".format(table_name)
cursor.execute(select_sql_string)
result = cursor.fetchall()
for row in result:
    print(row)

"""
(1, 'Super Pete', 'James Dean', 2020)
(2, 'Big Easter Adventure', 'James Dean', 2014)
(3, 'I Love My White Shoes', 'James Dean', 2010)
"""

description = cursor.description
print(description)
column_names = [row[0] for row in description]
for row in description:
    print(row[0])

books = pd.DataFrame(result, columns=column_names)
print(books.head())

"""
   id                  title      author  publication_year
0   1             Super Pete  James Dean              2020
1   2   Big Easter Adventure  James Dean              2014
2   3  I Love My White Shoes  James Dean              2010
"""

# Count total rows of books table
select_sql_string = "SELECT COUNT(1) AS cnt, COUNT(DISTINCT id) AS books FROM {}".format(table_name)
cursor.execute(select_sql_string)
result = cursor.fetchone()
print("How many books are in the library database: {}".format(result[1]))

# Update data in the table
update_sql_string = "UPDATE {} SET publication_year = ? WHERE id = ?".format(table_name)
cursor.execute(update_sql_string, (2023, 1))
conn.commit()

# Read and print all books from the table after the update
select_sql_string = "SELECT * FROM {}".format(table_name)
cursor.execute(select_sql_string)
result = cursor.fetchall()
for row in result:
    print(row)

"""
(1, 'Super Pete', 'James Dean', 2023)
(2, 'Big Easter Adventure', 'James Dean', 2014)
(3, 'I Love My White Shoes', 'James Dean', 2010)
"""

description = cursor.description
print(description)
column_names = [row[0] for row in description]
for row in description:
    print(row[0])

books = pd.DataFrame(result, columns=column_names)
print(books.head())

"""
   id                  title      author  publication_year
0   1             Super Pete  James Dean              2023
1   2   Big Easter Adventure  James Dean              2014
2   3  I Love My White Shoes  James Dean              2010
"""

# Delete data from the table
delete_sql_string = "DELETE FROM {} WHERE title = ?".format(table_name)
cursor.execute(delete_sql_string, ('Super Pete',))
conn.commit()

# Read and print all books from the table after the deletion
select_sql_string = "SELECT * FROM {}".format(table_name)
cursor.execute(select_sql_string)
result = cursor.fetchall()
for row in result:
    print(row)

description = cursor.description
print(description)
column_names = [row[0] for row in description]
for row in description:
    print(row[0])

books = pd.DataFrame(result, columns=column_names)
print(books.head())

# Close the cursor
cursor.close()

# Close the connection
conn.close()

if os.path.exists(db_name):
    print("The database exists.")
    os.remove(db_name)
    print("The database is deleted.")
else:
    print("The database does not exist.")
