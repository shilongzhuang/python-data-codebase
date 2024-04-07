import sqlite3
import os
import pandas as pd
import numpy as np

sqlite_default_db_name = 'default'
sqlite_db_extension = '.db'
sqlite_default_db_file = sqlite_default_db_name + sqlite_db_extension
print(sqlite_default_db_file)


def create_sqlite_connection(db_file):
    """
    Function to create sqlite connection from a local file
    Parameters:
        db_file (str): a local file name ends with .db
    Returns:
        sqlite3.Connection: SQLite connection, otherwise an empty list.
    """
    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect(db_file)
        return sqlite_connection
    except sqlite3.Error as e:
        print("SQLite error:", e)
    return sqlite_connection


def close_sqlite_connection(connection):
    """
    Function to close a sqlite connection
    Parameters:
        connection (sqlite3.Connection): SQLite connection
    Returns: None
    """
    if connection:
        try:
            connection.close()
            return True
        except sqlite3.Error as e:
            print("SQLite error:", e)
    return False


def drop_sqlite_database(db_file):
    """
    Function to drop sqlite by deleting the local file
    Parameters:
        db_file (str): a local file name ends with .db
    Returns: None
    """
    try:
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"The existing database '{db_file}' dropped successfully.")
        else:
            print("The database does not exist.")
    except FileNotFoundError:
        print(f"Database '{db_file}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def execute_sqlite_query(connection, query, params=None, batch_insert=False):
    """
    Function to execute SQLite queries.
    Parameters:
        connection (sqlite3.Connection): SQLite connection
        query (str): SQL query to execute.
        params (list of tuples): Parameters to pass with the query (optional).
        batch_insert (bool): Whether to perform batch insert (default: False).
    Returns:
        list: Result of the query if any, otherwise an empty list.
    """
    cursor = connection.cursor()
    # is_select_sql = query.strip().upper().startswith('SELECT')
    # may also need to consider the CTE cases
    is_return_sql = any(query.strip().upper().startswith(keyword) for keyword in ['SELECT', 'PRAGMA'])
    # Execute the query
    if params:
        if batch_insert:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)
    else:
        cursor.execute(query)

    # If the query is a SELECT statement, fetch the results
    if is_return_sql:
        cursor.execute(query)
        description = cursor.description
        columns = [row[0] for row in description]
        records = cursor.fetchall()
        return records, columns

    # Commit changes for non-SELECT queries
    if not is_return_sql:
        connection.commit()

    # Close cursor
    cursor.close()

    return None


# Create a sqlite default database default.db
conn = create_sqlite_connection(sqlite_default_db_file)
# Show all database command to list the db file paths
sqlite_show_database_query = 'PRAGMA database_list;'

# In this method the cursor needs to be closed each time
result, column_names = execute_sqlite_query(connection=conn, query=sqlite_show_database_query)

df = pd.DataFrame(result, columns=column_names)
print(df.head())

for row in result:
    seq, name, file = row[0], row[1], row[2]
    db_file = file.split('/')[-1]
    db_name = db_file.replace(sqlite_db_extension, '')
    print(seq, name, file, db_file, db_name)


def list_database_names(connection):
    database_names = []
    sqlite_show_database_query = 'PRAGMA database_list;'
    result, column_names = execute_sqlite_query(connection=connection, query=sqlite_show_database_query)
    for row in result:
        seq, name, file = row[0], row[1], row[2]
        database_file = file.split('/')[-1]
        database_name = database_file.replace(sqlite_db_extension, '')
        print(seq, name, file, database_file, database_name)
        database_names.append(database_name)
    return database_names


database_names = list_database_names(connection=conn)
print(database_names)

# Close the sqlite connection
close_sqlite_connection(connection=conn)
drop_sqlite_database(sqlite_default_db_file)
