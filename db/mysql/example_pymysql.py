import pymysql
import pymysql.cursors
import os

mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_port = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")

conn = pymysql.connect(host=mysql_host,
                       user=mysql_user,
                       password=mysql_password,
                       database=mysql_database,
                       cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = "SELECT * FROM users"
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            print(row)
finally:
    cursor.close()
    conn.close()
