from . import re
from . import data_type_conversions
import sqlite3 as sql


# @time_all_class_methods
class Sql:
    def __init__(self, database=None, console_output=False):
        self.master_table_dat = None
        self.console_output = console_output
        self.db = database
        self._set_con_and_cur()
        self._close_con_and_cur()

    def _close_con_and_cur(self):
        self.cur.close()
        self.conn.close()

    def _set_con_and_cur(self):
        self.conn = sql.connect(self.db)
        self.cur = self.conn.cursor()

    def run_query_text(self, query_text):
        self._set_con_and_cur()
        res = self.cur.execute(query_text)
        dat = res.fetchall()
        print(f'debug run query text before close\n{dat}')
        self._close_con_and_cur()
        print(f'debug run query text after close\n{dat}')
        print(dat)
        return dat

    def create_table(self, *args, **kwargs):
        columns = []
        table_name = ''
        if 'table_name' in kwargs and type(kwargs['table_name']) == str:
            table_name = kwargs['table_name'].replace(' ', '_')
        if 'columns' in kwargs and type(kwargs['columns']) == dict:
            for k, v in kwargs['columns'].items():
                columns.append(f'{k} {data_type_conversions[v.__name__]}')
        column_text = ','.join(columns)
        new_table_text = f'create table {table_name} ({column_text})'
        print(f'debug new_table_text: "{new_table_text}"')
        self.run_query_text(new_table_text)

        # print(f'debugging *args {args}')
        # for arg in args:
        #     print(f'arg: {arg}')
        #
        # for kwarg in kwargs:
        #     print(f'kwarg: {kwarg} | kwargs[kwarg]: {kwargs[kwarg]}')
        # for k, v in kwargs.items():
        #     print(f'k: {k} | v: {v}')

    def populate_table(self, *args, **kwargs):
        inval = data_type_conversions
        value_markers = []
        table_name = ''  # todo: validate that the table name exists in the database.
        for arg in args:
            print(f'debug arg in args: {arg}')
            if type(arg) == list:
                value_markers = ['?' for x in range(len(arg[0])) if type(arg[0]) == list]
        value_marker_text = ','.join(value_markers)
        print(f'debug value_marker_text: "{value_marker_text}"')
        insert_into_text = f'insert into {table_name} values ({value_markers})'
        print(f'debug insert_into_text: "{insert_into_text}"')
        # todo: find a way to use the cur.executemany(x, y) method instead of cur.execute(z)
        # self.run_query_text(insert_into_text)

    def run_query_file(self, file_path):
        with open(file_path) as f:
            res = self.run_query_text(f.read())
            # print(f'debugging res var in run_query_file method\n{res}')
            return res

    def get_existing_tables(self):
        query_text = '''select * from main.sqlite_master'''
        self._set_con_and_cur()
        res = self.cur.execute(query_text)
        self.master_table_dat = res.fetchall()
        self._close_con_and_cur()
