import sqlparse

sql = "/* comment*/ SELET column1, column2 FROM table WHERE condition = 1"
formatted_sql = sqlparse.format(sql, keyword_case="upper", strip_comments=True, reindent=False)
parsed_sql = sqlparse.parse(formatted_sql)
# print(parsed[0])

for token in parsed_sql[0].tokens:
    print(token, token.is_whitespace, token.is_keyword)