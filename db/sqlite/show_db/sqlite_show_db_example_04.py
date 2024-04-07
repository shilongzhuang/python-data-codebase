import sqlite3
import os
import pandas as pd

SQLITE_DIR_ENV_NAME = 'SQLITE_DIR'
SQLITE_DIR = os.getenv('SQLITE_DIR')
SQLITE_DB_EXTENSION = '.db'


def build_sqlite_db_path(default_db_name='default', default_path=None):
    """
    Function to build path for sqlite database
    Parameters:
        default_db_name (str): SQLite database name
        default_path (str): SQLite default path dir
    Returns: sqlite db path
    """
    if default_path is None:
        default_path = os.getenv(SQLITE_DIR_ENV_NAME)
        if default_path is None:
            default_path = ''
        # raise ValueError("Environment variable 'DB_DIRECTORY' not set.")
    # Build the full path based on the database file and default path
    if SQLITE_DB_EXTENSION not in default_db_name.lower():
        default_db_name = default_db_name + SQLITE_DB_EXTENSION
    db_path = os.path.join(default_path, default_db_name)
    return db_path


def create_sqlite_connection(name='default', path=None):
    """
    Function to create sqlite database
    Parameters:
        name (str): SQLite database name by default it is default
        path (str): SQLite default path dir
    Returns: connection (sqlite3.Connection): SQLite connection
    """
    db_path = build_sqlite_db_path(name, path)
    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect(db_path)
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


def drop_sqlite_database(name='default', path=None):
    """
    Function to drop sqlite by deleting the local file
    Parameters:
        name (str): a local file name ends with .db or maybe not
        path (str): default SQLITE DIR
    Returns: None
    """
    db_path = build_sqlite_db_path(name, path)
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"The existing database '{db_path}' dropped successfully.")
        else:
            print("The database does not exist.")
    except FileNotFoundError:
        print(f"Database '{db_path}' does not exist.")
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


def list_sqlite_database_names(db_path=None):
    # Create an empty list to store database file names
    if db_path is None:
        db_path = os.getenv(SQLITE_DIR_ENV_NAME)
        if db_path is None:
            db_path = ''
    db_names = []
    # Iterate over files in the directory
    if os.path.exists(db_path):
        for file_name in os.listdir(db_path):
            # Check if the file name contains the ".db" extension
            if file_name.endswith(SQLITE_DB_EXTENSION):
                # Add the file name to the list
                db_name = file_name.replace(SQLITE_DB_EXTENSION, '')
                db_names.append(db_name)
    return db_names


# Create default database name
conn = create_sqlite_connection()
# Create mydb database name
create_sqlite_connection('mydb')

sqlite_db_names = ['mydb', 'test', 'demo']

for db_name in sqlite_db_names:
    print(db_name)
    create_sqlite_connection(db_name)


sqlite_db_names = list_sqlite_database_names()
print('List', sqlite_db_names)
