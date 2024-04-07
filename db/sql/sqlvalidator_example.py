import sqlvalidator

try:
    sql = "SELECT * FROM table t"
    formatted_sql = sqlvalidator.format_sql(sql)
    print(formatted_sql)
except ValueError:
    print("Invalid SQL!")

try:
    sql = "SELEC * FROM table t"
    formatted_sql = sqlvalidator.format_sql(sql)
    print(formatted_sql)
except Exception as e:
    print("Invalid SQL!", e)

try:
    sql = "SELEC * FROM table t"
    parsed_sql = sqlvalidator.parse(sql)
    is_valid = parsed_sql.is_valid()
    if is_valid:
        print("Valid SQL!")
    else:
        print("Invalid SQL!")
except:
    pass
