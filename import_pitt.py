import os
import pandas as pd
from modules.sql import SqliteHandler
from modules.sql import MysqlHandler
from modules.project_enums import Engines
from modules.project_enums import HandlerParams
from modules.project_enums import SQLText
from modules.project_enums import Regex

env = os.environ
hp = HandlerParams


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


def update_pivot_tables():
    print('Collecting all distinct post types.')
    engines = establish_engines()
    pitt = engines[Engines.pitt_engine_name.value]
    lwp = engines[Engines.local_wp_engine_name.value]
    ptqt = SQLText.distinct_post_types.value.text
    post_type_df = pd.read_sql(ptqt, pitt.engine)
    type_list = [x[0] for x in post_type_df.values.tolist()]

    print('Collecting all the columns for each post type.')
    type_key_dfs = {}
    type_value_dfs = {}
    for post_type in type_list:
        if post_type not in type_key_dfs:
            type_key_dfs[post_type] = pd.read_sql(collect_meta_query_field_keys(post_type), pitt.engine)
        if post_type not in type_value_dfs:
            type_value_dfs[post_type] = pd.read_sql(collect_meta_query_field_values(post_type), pitt.engine)

    print('Pivoting the values for the post types and columns by post_id')
    type_key_pivot_dfs = {}
    type_value_pivot_dfs = {}
    for post_type in type_list:
        df_key = type_key_dfs[post_type]
        df_val = type_value_dfs[post_type]
        if post_type not in type_key_pivot_dfs and df_key.empty is not True:
            type_key_pivot_dfs[post_type] = df_key.pivot(index='post_id', columns='meta_key', values='meta_value')
        if post_type not in type_value_pivot_dfs and df_val.empty is not True:
            type_value_pivot_dfs[post_type] = df_val.pivot(index='post_id', columns='meta_key', values='meta_value')

    # todo: attempt to have wpengine whitelist the ipv4 address=67.161.213.7 instead of the two ipv6 addresses.

    # todo: Parse out the groups within the post type to avoid exceeding the max number of columns in a table.
    #   1. determine the group parents.
    #       if field in index loop?
    #       recursive method that pops and evaluates fields in a loop?
    #   2. organize the parents into their own df.
    #   3. Move children columns to their parent/group df.
    #   4. Create a pivot table of the group parents instead of the post types.

    # print('Updating the meta_pivot_(keys/values)_type with the pivot data.')
    # for table_suf, df in type_key_pivot_dfs.items():
    #     df.to_sql(f'meta_pivot_keys_{table_suf}',
    #               con=lwp.engine,
    #               schema='wp_pivot_data',
    #               if_exists='replace')
    # for table_suf, df in type_value_pivot_dfs.items():
    #     df.to_sql(f'meta_pivot_values_{table_suf}',
    #               con=lwp.engine,
    #               schema='wp_pivot_data',
    #               if_exists='replace')
    # return [post_type_df, type_list, type_value_pivot_dfs, type_key_pivot_dfs]


def melt_pivot_tables():
    print('Grabbing the list of pivot table names.')
    engines = establish_engines()
    locwp = engines[Engines.local_wp_engine_name.value]
    sqlite = engines[Engines.sqlite_engine_name.value]
    apt = SQLText.all_pivot_tables.value.text
    pt_df = pd.read_sql(apt, locwp.engine)
    table_names = [x[0] for x in pt_df.values.tolist()]
    # Why did I need the post types?
    post_types = [Regex.pivot_table_prefix.value.sub('', x) for x in table_names]

    print('Extracting the contents of each pivot table.')
    pivot_dfs = [pd.read_sql(f'select * from wp_pivot_data.`{x}`', locwp.engine) for x in table_names]

    print('Metling the extracted data.')
    melt_dfs = [df.melt(id_vars='post_id', var_name='meta_key', value_name='meta_value') for df in pivot_dfs]

    print('Backing up the current state of the postmeta table.')
    post_meta_backup = pd.read_sql('select * from wp_liftenergypitt.wp_postmeta', locwp.engine).set_index('meta_id')
    backups = pd.read_sql('select * from main.postmeta_backups', sqlite.engine)
    post_meta_backup.to_sql(f'postmeta_backup_{backups["backup_count"][0]}',
                            con=sqlite.engine,
                            if_exists='replace')
    backups["backup_count"][0] += 1
    backups.to_sql('postmeta_backups', con=sqlite.engine, if_exists='replace', index=False)

    # todo 4. Update the postmeta table with the melted data.
    print('Updating the postmeta table with the melted data.')
    for df in melt_dfs:
        df.to_sql('wp_postmeta',
                  con=locwp.engine,
                  schema='wp_liftenergypitt',
                  if_exists='append',
                  index=False)
    return [table_names, pivot_dfs, melt_dfs, post_meta_backup]


if __name__ == '__main__':
    print('work in progress')
    # tests_pivot = update_pivot_tables()
    # tests_melt = melt_pivot_tables()
