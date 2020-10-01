import os
from modules import pd
from modules import env
from modules import logging
from modules.sql import SqliteHandler
from modules.sql import MysqlHandler
from modules.project_enums import Engines
from modules.project_enums import SQLText
from modules.project_enums import Regex


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

    local_wp_host = '10.0.0.87'
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
    raw_query_text = SQLText.post_type_meta_collection_split.value.text
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
    raw_query_text = SQLText.post_type_meta_collection_split.value.text
    return raw_query_text % ('like \'\_%\'', post_type)


def update_pivot_tables():
    logging.info('Collecting all distinct post types.')
    engines = establish_engines()
    pitt = engines[Engines.pitt_engine_name.value]
    lwp = engines[Engines.local_wp_engine_name.value]
    ptqt = SQLText.distinct_post_types.value.text
    ptmcqt = SQLText.post_type_meta_collection_join.value.text
    post_type_df = pd.read_sql(ptqt, lwp.engine)
    type_list = [x[0] for x in post_type_df.values.tolist()]

    logging.info('Collecting all the columns for each post type.')
    type_dfs = {}
    type_key_dfs = {}
    type_value_dfs = {}
    for post_type in type_list:
        type_dfs[post_type] = pd.read_sql(ptmcqt % post_type, lwp.engine)
        if post_type not in type_key_dfs:
            type_key_dfs[post_type] = pd.read_sql(collect_meta_query_field_keys(post_type), lwp.engine)
        if post_type not in type_value_dfs:
            type_value_dfs[post_type] = pd.read_sql(collect_meta_query_field_values(post_type), lwp.engine)

    type_orgs = {}
    for x in [type_key_dfs, type_value_dfs]:
        logging.debug(f'iterating through {x.keys()}')
        for pt, pt_df in x.items():
            logging.debug(f'evaluating {pt}\n\tpt_df: {pt_df.head(1)}')
            if pt not in type_orgs:
                tmp_arg = pt_df['meta_key'].values.tolist()
                logging.debug(f'passing tmp_arg to sift_metadata_to_groups method\n\ttmp_arg: {tmp_arg}')
                pt_df_org = sift_metadata_to_groups(tmp_arg)
                logging.debug(f'evaluating the return of the sifting method\n\t{pt_df_org}')
                type_orgs[pt] = pt_df_org
            else:
                tmp_arg = pt_df['meta_key'].values.tolist()
                logging.debug(f'passing tmp_arg to sift_metadata_to_groups method\n\ttmp_arg: {tmp_arg}')
                pt_df_org = sift_metadata_to_groups(tmp_arg)
                logging.debug(f'evaluating the return of the sifting method\n\t{pt_df_org}')
                type_orgs[pt].update(pt_df_org)

    # This dictionary represents the table names and column names that will be represented in the pivot tables schema.
    type_group_table_org = {}
    for pt, ptg in type_orgs.items():
        for group, cols in ptg.items():
            table_columns = cols
            if group == 'f':
                table_name = pt
            else:
                table_name = f'{pt}_{group}'
            type_group_table_org[table_name] = table_columns

    logging.info('Pivoting the values for the post types and columns by post_id')
    type_key_pivot_dfs = {}
    type_value_pivot_dfs = {}
    for post_type in type_list:
        df_key = type_key_dfs[post_type]
        df_val = type_value_dfs[post_type]
        if post_type not in type_key_pivot_dfs and df_key.empty is not True:
            type_key_pivot_dfs[post_type] = df_key.pivot(index='post_id', columns='meta_key', values='meta_value')
        if post_type not in type_value_pivot_dfs and df_val.empty is not True:
            type_value_pivot_dfs[post_type] = df_val.pivot(index='post_id', columns='meta_key', values='meta_value')
    type_pivot_dfs = {}
    # for tl in type_list:
    # type_pivot_dfs = pd.concat([type_key_pivot_dfs[tl], type_value_pivot_dfs[tl]], axis=1)

    logging.debug('Attempting to parse out the pivoted dataframes into their own respective type_group dataframes')

    pivot_tables = {}

    # logging.info('Updating the meta_pivot_(keys/values)_type with the pivot data.')
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
    return [post_type_df,
            type_list,
            type_key_dfs,
            type_value_dfs,
            type_value_pivot_dfs,
            type_key_pivot_dfs,
            type_orgs,
            type_group_table_org,
            type_pivot_dfs]


def melt_pivot_tables():
    logging.info('Grabbing the list of pivot table names.')
    engines = establish_engines()
    locwp = engines[Engines.local_wp_engine_name.value]
    sqlite = engines[Engines.sqlite_engine_name.value]
    apt = SQLText.all_pivot_tables.value.text
    pt_df = pd.read_sql(apt, locwp.engine)
    table_names = [x[0] for x in pt_df.values.tolist()]
    # Why did I need the post types?
    post_types = [Regex.pivot_table_prefix.value.sub('', x) for x in table_names]

    logging.info('Extracting the contents of each pivot table.')
    pivot_dfs = [pd.read_sql(f'select * from wp_pivot_data.`{x}`', locwp.engine) for x in table_names]

    logging.info('Metling the extracted data.')
    melt_dfs = [df.melt(id_vars='post_id', var_name='meta_key', value_name='meta_value') for df in pivot_dfs]

    logging.info('Backing up the current state of the postmeta table.')
    post_meta_backup = pd.read_sql('select * from wp_liftenergypitt.wp_postmeta', locwp.engine).set_index('meta_id')
    backups = pd.read_sql('select * from main.postmeta_backups', sqlite.engine)
    post_meta_backup.to_sql(f'postmeta_backup_{backups["backup_count"][0]}',
                            con=sqlite.engine,
                            if_exists='replace')
    backups["backup_count"][0] += 1
    backups.to_sql('postmeta_backups', con=sqlite.engine, if_exists='replace', index=False)

    # todo 4. Update the postmeta table with the melted data.
    logging.info('Updating the postmeta table with the melted data.')
    for df in melt_dfs:
        df.to_sql('wp_postmeta',
                  con=locwp.engine,
                  schema='wp_liftenergypitt',
                  if_exists='append',
                  index=False)
    return [table_names, pivot_dfs, melt_dfs, post_meta_backup]


def sift_metadata_to_groups(name_list=None, group_dict=None):
    if group_dict is None:
        group_dict = {}
    if name_list is None:
        name_list = [
            # A table would be the name of the entity/group, and the columns of the table
            # would be the fields of the group.
            #
            # If an item occurs only once in the set, then it is a field.
            # If an item occurs more than once in the set, then it is a group/table.
            # The parent group of the field is the group, with the largest length of string,
            # that matches part of the field name.
            # The matching field/group combinations should be removed from the list, and organized into
            # a dictionary.
            #
            # The recursive function would be passed a list of strings and dictionary of data,
            # determine which indexes in the list are fields and groups,
            # remove the field/group pairs from the list into the dictionary,
            # then pass the new list & dictionary combo back to the function.
            # The conditional statement would be if the list of strings is empty.
            'tp_a',  # group
            'tp_a_field1',  # field
            'tp_a_group1',  # group
            'tp_a_group1_field1',  # field
            'tp_a_group1_field2_group',  # group
            'tp_a_group1_field2_group_field0',  # field
            'tp_a_group1_field2_group_field1',  # field
            'tp_a_group1_field3',  # field
            'tp_a_group2',  # group
            'tp_a_group2_field1',  # field
            'tp_c',  # group
            'tp_c_group1',  # field
        ]
        example_output = {
            'tp_a': ['tp_a_field1'],
            'tp_a_group1': ['tp_a_group1_field1', 'tp_a_group1_field3'],
            'tp_a_group1_field2_group': ['tp_a_group1_field2_group_field1', 'tp_a_group1_field2_group_field2'],
            'tp_a_group2': ['tp_a_group2_field1'],
            'tp_c': ['tp_c_group1'],
        }
    if len(name_list) == 0:  # control statement
        return group_dict
    else:  # primary method
        logging.debug('Sifting the name list provided.')
        labels = {}

        logging.debug('Determining which indexes in the name list is a field and which is a group.')
        for i in range(len(name_list)):
            ict = 0
            for j in range(len(name_list)):
                if name_list[i] in name_list[j]:
                    ict += 1
            if ict > 1:
                labels[name_list[i]] = 'g'
            else:
                labels[name_list[i]] = 'f'
        groups = {key: value for (key, value) in labels.items() if value == 'g'}
        fields = {key: value for (key, value) in labels.items() if value == 'f'}

        logging.debug('Matching the parent group to each field.')
        for group, glab in groups.items():
            for field, flab in fields.items():
                if group in field and len(group) > len(flab):
                    fields[field] = group

        logging.debug('Generating a dictionary of group names and a list of their columns.')
        for field, parent in fields.items():
            if parent not in group_dict:
                group_dict[parent] = [field]
            else:
                group_dict[parent].append(field)
        # todo: parse out just the group names from the group_dict to avoid table names > 64 characters long.
        logging.debug('Sift complete, returning the list\'s groups and their columns.')
        return group_dict


if __name__ == '__main__':
    logging.debug('Work in Progress')
    engines = establish_engines()
    pl = engines[Engines.local_wp_engine_name.value]
    pitt = engines[Engines.pitt_engine_name.value]
    lite = engines[Engines.sqlite_engine_name.value]

    tests = update_pivot_tables()
    ptdf = tests[0]
    tl = tests[1]
    tkdfs = tests[2]
    tvdfs = tests[3]
    tvpdfs = tests[4]
    tkpdfs = tests[5]
    tos = tests[6]
    tgto = tests[7]
    tpd = tests[8]

    sys = tkdfs['system']
    sys_names = sys['meta_key'].values.tolist()
    sys_org_test = sift_metadata_to_groups(sys_names)
    sysp = tkpdfs['system']
    # to avoid info debugging with every run and speed up execution time, consult the following link:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
    sys_homeowner_df = sysp.loc[:, sys_org_test['_tp_homeowner']]
    sys_homeowner_df['_tp_homeowner'] = sysp.loc[:, '_tp_homeowner']

    # success! tkpdfs['system'][sys_org_test[3]['_tp_homeowner']]
