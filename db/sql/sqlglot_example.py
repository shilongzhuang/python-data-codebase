import sqlglot
from sqlglot import Tokenizer

tokenizer = Tokenizer()

sql = "/* comment*/ SELEct column1, column2 FROM table WHERE condition = 1"
try:
    sqlglot.transpile(sql)
    print("Valid SQL!")
except sqlglot.errors.ParseError as e:
    print(e.errors)

tokens = tokenizer.tokenize(sql)
for token in tokens:
    print(token)
    print("Token type: ", token.token_type, token.token_type.name)
    print("Token text: ", token.text, token.text.upper())

sql = "/* comment*/ SELET column1, column2 FROM table WHERE condition = 1"
try:
    sqlglot.transpile(sql)
    print("Valid SQL!")
except sqlglot.errors.ParseError as e:
    print(e.errors)


def is_valid(sql):
    valid = True
    try:
        sqlglot.transpile(sql)
        return valid
    except sqlglot.errors.ParseError as e:
        valid = False
    return valid


sql = "SELECT 1 FROM table WHERE 1=1 AND city = 'SFO'"
print(sql, is_valid(sql))

sql = "SELEC 1 from Table"
print(sql, is_valid(sql))
