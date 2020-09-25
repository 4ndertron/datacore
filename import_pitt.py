import os
import pandas as pd
from sqlalchemy import create_engine
from modules.project_enums import Engines
from modules.project_enums import HandlerParams
from modules.project_enums import Messages
from modules.project_enums import SQLText
from modules.project_enums import Regex

env = os.environ
hp = HandlerParams


class SqliteHandler:
    def __init__(self, **kwargs):
        self.default = 'sqlite:///./data/foo.db'
        self.name = Engines.sqlite_engine_name.value
        self.engine = None
        self._setup_engine()

    def _setup_engine(self):
        self.engine = create_engine(self.default)


class MysqlHandler:
    def __init__(self, **kwargs):
        self._host = kwargs[hp.host.value] if hp.host.value in kwargs else 'localhost'
        self._port = kwargs[hp.port.value] if hp.port.value in kwargs else '3306'
        self._user = kwargs[hp.user.value] if hp.user.value in kwargs else 'root'
        self._pswd = kwargs[hp.pswd.value] if hp.pswd.value in kwargs else 'root'
        self.name = kwargs[hp.name.value] if hp.name.value in kwargs else 'mysql'
        self.valid_parameters = HandlerParams.valid_params.value
        self.engine = None
        self._setup_engine()

    def _setup_engine(self):
        url = f'mysql://{self._user}:{self._pswd}@{self._host}:{self._port}'
        self.engine = create_engine(url)

    def update_connection_parameters(self, **kwargs):
        self._host = kwargs[hp.host.value] if hp.host.value else self._host
        self._port = kwargs[hp.port.value] if hp.port.value else self._port
        self._user = kwargs[hp.user.value] if hp.user.value else self._user
        self._pswd = kwargs[hp.pswd.value] if hp.pswd.value else self._pswd
        self.name = kwargs[hp.name.value] if hp.name.value in kwargs else self.name
        returns = []
        for k, v in kwargs:
            if k in self.valid_parameters:
                returns.append(k)
        if len(returns) == 0:
            return Messages.no_valid_parameters.value
        else:
            self._setup_engine()
            return f'{Messages.updated_valid_parameters.value}{",".join(returns)}'

    def get_database_outline(self):
        data = {}
        if self.engine is not None:
            return 'success'


def establish_engines():
    pitt_host = env['thepitt_db_host']
    pitt_port = env['thepitt_db_port']
    pitt_user = env['thepitt_db_user']
    pitt_pswd = env['thepitt_db_pswd']
    pitt_name = Engines.pitt_engine_name.value

    pitt_engine = MysqlHandler(
        host=pitt_host,
        port=pitt_port,
        user=pitt_user,
        pswd=pitt_pswd,
        name=pitt_name,
    )

    local_wp_host = '10.1.10.38'
    local_wp_port = '23306'
    local_wp_user = 'wordpress'
    local_wp_pswd = 'wordpress'
    local_wp_name = Engines.local_wp_engine_name.value

    local_wp_engine = MysqlHandler(
        host=local_wp_host,
        port=local_wp_port,
        user=local_wp_user,
        pswd=local_wp_pswd,
        name=local_wp_name,
    )

    sqlite_engine = SqliteHandler()
    return {local_wp_engine.name: local_wp_engine,
            pitt_engine.name: pitt_engine,
            sqlite_engine.name: sqlite_engine}


def generate_union_statement(columns, source):
    """

    This function may not be needed depending on the results found in the melt_test() function

    This function is intended to handle the dynamic and tedious task of maintaining the union all
    sql statements used to import data into the word press engine's wp_postmeta table.
    :param columns: an iterable data type (array/tuple) containing all of the column names.
    :param source: the explicit 'database.table' source text used in the queries WHERE clause.
    :return: text type of the entire union statement query.
    """
    sql_raw_text = SQLText.union_part.value
    if len(columns) >= 2:
        union_parts = []
        for column in columns:
            if len(column) >= 3 and type(column) == 'str':
                union_part_text = sql_raw_text.text % (column, column, source)
                union_parts.append(union_part_text)
            full_union_text = 'union all'.join(union_parts)
            return full_union_text
    else:
        full_union_text = sql_raw_text.text % (columns[0], columns[0], source)
        return full_union_text


def update_pivot_tables():
    # todo: 1. Collect all distinct post types
    engines = establish_engines()
    pitt = engines[Engines.pitt_engine_name.value]
    lwp = engines[Engines.local_wp_engine_name.value]
    ptqt = SQLText.distinct_post_types.value.text
    post_type_df = pd.read_sql(ptqt, pitt.engine)
    type_list = [x[0] for x in post_type_df.values.tolist()]

    # todo: 2. Collect all columns for each post type
    type_key_dfs = {}
    type_value_dfs = {}
    for post_type in type_list:
        if post_type not in type_key_dfs:
            type_key_dfs[post_type] = pd.read_sql(collect_meta_query_field_keys(post_type), pitt.engine)
        if post_type not in type_value_dfs:
            type_value_dfs[post_type] = pd.read_sql(collect_meta_query_field_values(post_type), pitt.engine)

    # todo: 3. pivot the values for the post types and columns by post_id
    type_key_pivot_dfs = {}
    type_value_pivot_dfs = {}
    for post_type in type_list:
        df_key = type_key_dfs[post_type]
        df_val = type_value_dfs[post_type]
        if post_type not in type_key_pivot_dfs and df_key.empty is not True:
            type_key_pivot_dfs[post_type] = df_key.pivot(index='post_id', columns='meta_key', values='meta_value')
        if post_type not in type_value_pivot_dfs and df_val.empty is not True:
            type_value_pivot_dfs[post_type] = df_val.pivot(index='post_id', columns='meta_key', values='meta_value')

    # todo: 4. update the type_meta_pivot_values with the pivot
    for table_suf, df in type_key_pivot_dfs.items():
        df.to_sql(f'meta_pivot_keys_{table_suf}',
                  con=lwp.engine,
                  schema='wp_pivot_data',
                  if_exists='replace')
    for table_suf, df in type_value_pivot_dfs.items():
        df.to_sql(f'meta_pivot_values_{table_suf}',
                  con=lwp.engine,
                  schema='wp_pivot_data',
                  if_exists='replace')
    return [post_type_df, type_list, type_value_pivot_dfs, type_key_pivot_dfs]


def collect_meta_query_field_values(post_type):
    """
    This method will return the query text required to collect meta post data based off
    of a post_type as a string.
    This method is required because the sql wildcard character '%' was being picked up
    by python's string interpolation as a format occurrence. This method will automatically
    handle the wildcard character interactions with the query text.

    :param post_type: string of post type contained in the wp posts table.
    :return: query text.
    """
    raw_query_text = SQLText.post_type_meta_collection.value.text
    return raw_query_text % ('not like \'\_%\'', post_type)


def collect_meta_query_field_keys(post_type):
    """
    This method will return the query text required to collect meta post data based off
    of a post_type as a string.
    This method is required because the sql wildcard character '%' was being picked up
    by python's string interpolation as a format occurrence. This method will automatically
    handle the wildcard character interactions with the query text.

    :param post_type: string of post type contained in the wp posts table.
    :return: query text.
    """
    raw_query_text = SQLText.post_type_meta_collection.value.text
    return raw_query_text % ('like \'\_%\'', post_type)


def melt_pivot_tables():
    # todo: 1. grab the list of pivot table names
    engines = establish_engines()
    locwp = engines[Engines.local_wp_engine_name.value]
    apt = SQLText.all_pivot_tables.value.text
    pt_df = pd.read_sql(apt, locwp.engine)
    table_names = [x[0] for x in pt_df.values.tolist()]
    # Why did I need the post types?
    post_types = [Regex.pivot_table_prefix.value.sub('', x) for x in table_names]

    # todo 2. Extract the contents of each table.
    pivot_dfs = [pd.read_sql(f'select * from wp_pivot_data.`{x}`', locwp.engine) for x in table_names]
    # todo 3. Melt the extracted data
    melt_dfs = [df.melt(id_vars='post_id', var_name='meta_key', value_name='meta_value') for df in pivot_dfs]
    # todo 4. Update the postmeta table with the melted data.
    return [table_names, post_types, pivot_dfs, melt_dfs]


if __name__ == '__main__':
    tests = melt_pivot_tables()
    t1 = tests[0]
    t2 = tests[1]
    t3 = tests[2]
    t4 = tests[3]
