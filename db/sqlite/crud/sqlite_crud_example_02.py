import sqlite3
import os
import pandas as pd

SQLITE_DB_EXTENSION = '.db'
# SQLITE_DIR = 'SQLITE_DIR'
SQLITE_DIR = ''


def build_sqlite_db_path(default_db_name='default', default_path=None):
    default_path = os.getenv(SQLITE_DIR)
    if default_path is None:
        default_path = ''
        # raise ValueError("Environment variable 'DB_DIRECTORY' not set.")
    # Build the full path based on the database file and default path
    if SQLITE_DB_EXTENSION not in default_db_name.lower():
        default_db_name = default_db_name + SQLITE_DB_EXTENSION
    db_path = os.path.join(default_path, default_db_name)
    return db_path


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


def create_sqlite_table(connection, create_table_sql):
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
    except sqlite3.Error as e:
        print("SQLite error:", e)


def drop_sqlite_table(connection, drop_table_sql):
    try:
        cursor = connection.cursor()
        cursor.execute(drop_table_sql)
    except sqlite3.Error as e:
        print("SQLite error:", e)


# Example usage:
db_name = 'mydb.db'
table_name = 'countries'
conn = create_sqlite_connection(db_name)
# print(type(conn))
# Create table if it doesn't exist
sql = """
CREATE TABLE IF NOT EXISTS {} (
                    alpha_2_code TEXT PRIMARY KEY,
                    name TEXT,
                    alpha_3_code TEXT,
                    numeric TEXT
                );
""".format(table_name)
try:
    execute_sqlite_query(connection=conn, query=sql)
except sqlite3.Error as e:
    print("SQLite error:", e)
finally:
    close_sqlite_connection(connection=conn)

# Example usage:
db_name = 'mydb.db'
table_name = 'countries'
conn = create_sqlite_connection(db_name)
# Insert data into table and Batch insert example
country_data = [
    ('US', 'United States', 'USA', '840'),
    ('CA', 'Canada', 'CAN', '124'),
    ('MX', 'Mexico', 'MEX', '484')
]
query = "INSERT OR IGNORE INTO {} (alpha_2_code, name, alpha_3_code, numeric) VALUES (?, ?, ?, ?)".format(table_name)

try:
    execute_sqlite_query(connection=conn, query=query, params=country_data, batch_insert=True)
    query = "SELECT * FROM {}".format(table_name)
    result, column_names = execute_sqlite_query(connection=conn, query=query)
    print(result, column_names)

    df = pd.DataFrame(result, columns=column_names)
    print(df.head())
    """
          alpha_2_code           name alpha_3_code numeric
    0           US  United States          USA     840
    1           CA         Canada          CAN     124
    2           MX         Mexico          MEX     484
    """
except sqlite3.Error as e:
    print("SQLite error:", e)
finally:
    close_sqlite_connection(connection=conn)

drop_sqlite_database(db_name)
