import sqlite3
import os
import pandas as pd


# Create sqlite connection based on local file
def create_sqlite_connection(db_file):
    sqlite_connection = None
    try:
        sqlite_connection = sqlite3.connect(db_file)
        return sqlite_connection
    except sqlite3.Error as e:
        print("SQLite error:", e)
    return sqlite_connection


# Close sqlite connection
def close_sqlite_connection(connection):
    if connection:
        try:
            connection.close()
            return True
        except sqlite3.Error as e:
            print("SQLite error:", e)
    return False


# Drop sqlite by deleting the local file
def drop_sqlite_database(db_file):
    try:
        os.remove(db_file)
        print(f"Database '{db_file}' dropped successfully.")
    except FileNotFoundError:
        print(f"Database '{db_file}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


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


# Execute sqlite query
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
    is_select_sql = query.strip().upper().startswith('SELECT')
    # Execute the query
    if params:
        if batch_insert:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)
    else:
        cursor.execute(query)

    # If the query is a SELECT statement, fetch the results
    if is_select_sql:
        cursor.execute(query)
        description = cursor.description
        columns = [row[0] for row in description]
        records = cursor.fetchall()
        return records, columns

    # Commit changes for non-SELECT queries
    if not is_select_sql:
        connection.commit()

    return None


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
