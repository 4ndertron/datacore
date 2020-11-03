# -*-coding:utf8;-*-
# qpy:3
# qpy:console

import os
import sqlite3 as sql
from old_modules import project_dir
from old_modules import database_dir


class Country:
    def __init__(self, name=''):
        self.name = name
        self.products = {}

    def add_product(self, product='', volume=0):
        if product in self.products:
            return -1
        self.products[product] = volume


class Sql:
    def __init__(self):
        self.db_name = 'super.sqlite'
        self.con = sql.connect(self.db_name)
        self.cur = self.con.cursor()

    def _close_con_and_cur(self, commit_changes=True):
        if commit_changes:
            self.con.commit()
        self.cur.close()
        self.con.close()


if __name__ == '__main__':

    relpath = './projects3/Economics'

    os.chdir(relpath)

    prod = {
        'USA': {
            'tomatoes': 261,
            'carrots': 278
        },
        'DE': {
            'tomatoes': 99,
            'carrots': 86
        }
    }

    conversions = {
        int: 'INTEGER',
        str: 'TEXT',
        list: 'TEXT',
        float: 'REAL',
    }

    product = []
    columns = []
    data = []

    # setup product list for columns
    for c in prod:
        for p in prod[c]:
            if p not in product:
                product.append(p)

    # setup column structure
    for i in range(len(product)):
        corner = 'country'
        col = product[i]
        if corner not in columns:
            columns.append(corner)
        if col not in columns:
            columns.append(col)

    data.append(columns)

    # setup core data
    for c in prod:
        new_row = [c]
        coun = prod[c]
        for p in coun:
            new_row.append(coun[p])
        data.append(new_row)

    # opportunity cost calculation
    for i in range(len(data)):
        row = data[i]
        if i == 0:
            row.append(f'{row[1]} opp cost')
            row.append(f'{row[2]} opp cost')
        else:
            row.append(int(row[2]) / int(row[1]))
            row.append(int(row[1]) / int(row[2]))

        print(row)

        for col in range(len(row)):
            print(f'{row[col]} has type of {conversions[type(row[col]).__name__]}')
    # print(product)
    # print(columns)

    # table_name
    tn = 'country_veg_production'
    # columns
    cs = ''
    # value markers
    vm = ''

    for i in range(len(columns)):
        r = data[1]
        col = [c.replace(' ', '_') for c in columns]
        dtype = conversions[type(r[i]).__name__]
        if i == len(col) - 1:
            cs = cs + f'{col[i]} {dtype}'
            vm = vm + '?'
        else:
            cs = cs + f'{col[i]} {dtype}, '
            vm = vm + '?,'

    # new_table_text
    ntt = f'create table {tn} ({cs})'
    print(ntt)

    # insert into text
    iit = f'insert into {tn} values ({vm})'
    # insert into values
    iiv = [tuple(x) for x in data[1:]]

    print(iit)
    print(iiv)

    # Start logging code grouping
    print(os.getcwd())
    con = sql.connect('super.sqlite')
    cur = con.cursor()
    cur.execute(ntt)
    cur.executemany(iit, iiv)
    con.commit()
    cur.close()
    con.close()
