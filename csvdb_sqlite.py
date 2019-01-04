import argparse
import csv
import sqlite3 as lite
import os

import sqlparser
import sqltokenizer

database_file = "demo.db"

class LiteCreate(sqlparser.BaseSyntaxNode):
    """Mock syntax tree node for the CREATE command
    self.text keeps the SQL CREATE command in SQLite dialect (after translation)"""
    def __init__(self, text):
        self.text = text

class LiteSelect(sqlparser.BaseSyntaxNode):
    """Mock syntax tree node for the SELECT command.
    No need to parse all the command token by token as it is passed
    as a string to SQLlite for execution"""
    def __init__(self, text):
        self.text = text


class LiteParser(sqlparser.SqlParser):
    """A mock parser to allow translate CSVDB SQL dialect to SQLite dialect."""
    def __init__(self, text):
        super().__init__(text)

    def _parse_create(self):
        tokens = []
        # translate SQLite types to CSVDB types
        lite_translate = {
            'varchar': 'text',
            'float': 'real',
            'int': 'integer',
            'timestamp': 'integer',
        }
        while self._token != sqltokenizer.SqlTokenKind.EOF:
            if self._val == ";":
                break  # end of current statement
            v = self._val
            if v in lite_translate:
                v = lite_translate[v]
            tokens.append(v)
            self._next_token()
        # rebuild the SQL command from the tokens.
        # now it is translated to the dialect of SQLite
        return LiteCreate(" ".join(tokens))

    def _parse_select(self):
        tokens = []
        while self._token != sqltokenizer.SqlTokenKind.EOF:
            if self._val == ";":
                break  # end of current statement
            v = self._val
            tokens.append(v)
            self._next_token()
        # rebuild the SQL command from the tokens.
        return LiteCreate(" ".join(tokens))


def execute_parsed_command(node, cursor, verbose):
    sql_text = None
    if isinstance(node, sqlparser.NodeDrop):
        not_exist_clause = ""
        if node.allow_not_exists:
            not_exist_clause = "IF EXISTS "
        sql_text = "DROP TABLE {}{};".format(not_exist_clause, node.table_name)
    elif isinstance(node, sqlparser.NodeLoad):
        # Read CSV and load into SQLite
        with open(node.infile_name, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for i, row in enumerate(reader):
                row = map(lambda s: '"' + s +'"' if s else "NULL", row)
                if i <= node.ignore_lines:
                    continue
                sql_text = "INSERT INTO {} VALUES ({});".format(node.table_name, ", ".join(row))
                cursor.execute(sql_text)
        return
    elif isinstance(node, LiteCreate):
        sql_text = node.text
    elif isinstance(node, LiteSelect):
        sql_text = node.text
    else:
        print("Bad node: ", node)
    if sql_text:
        if verbose:
            print("SQLite text: ", sql_text)
        cursor.execute(sql_text)
        fields = []
        if cursor.description:
            fields = [t[0] for t in cursor.description]
        if not fields:
            print("EMPTY RESULT")
            return
        print(fields)
        print(cursor.execute(sql_text).fetchall())


def sql_execute(sql_text, verbose):
    parser = LiteParser(sql_text)
    nodes = parser.parse_multi_commands()
    with lite.connect(database_file) as con:
        cur = con.cursor()
        for node in nodes:
            execute_parsed_command(node, cur, verbose)


def text_from_keyboard():
    print("Enter text. Finishh with a line containin ONLY ;")
    text = ""
    while True:
        line = input("csvdb> ")
        text += line + "\n"
        if line == ";":
            return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rootdir", help="Directory under which all table directories reside.", default=".")
    parser.add_argument("--run", help="execute all SQL commands inside FILENMAE. If one fails - stop execution")
    parser.add_argument("--verbose", help="print log messages that helps debug", action='store_true')
    parser.add_argument("--test", help="print log messages that helps debug", action='store_true')
    args = parser.parse_args()
    os.chdir(args.rootdir)

    if args.run:
        with open(args.run) as infile:
            sql_text = infile.read()
    else:
        sql_text = text_from_keyboard()
    if args.verbose:
        print("EXECUTING SQL: ", sql_text)
    sql_execute(sql_text, args.verbose)

# to pack files:
#$ zip csvdb_examples *.py *.sql *.csv

if __name__ == '__main__':
  main()
