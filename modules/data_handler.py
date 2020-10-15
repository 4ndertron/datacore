from modules import dt
from modules import pd
from modules import env
from modules import logging
from modules import json
from modules.sql_engines import SqliteHandler
from modules.sql_engines import MysqlHandler
from modules.sql_engines import EngineHandler
from modules.project_enums import Engines
from modules.project_enums import SQLText
from modules.project_enums import Regex
from modules.project_enums import DateTimes


class DataHandler:
    def __init__(self, *args, **kwargs):
        self.engines = {}
        self.creds = kwargs['creds'] if 'creds' in kwargs else None
        self.primary_backup_engine = EngineHandler(dialect='sqlite', database='./data/foo.db')
        self._establish_engines()

    def _establish_engines(self):
        for engine, params in self.creds.items():
            dialect = params['dialect']
            driver = params['driver']
            user = params['user']
            pswd = params['pswd']
            host = params['host']
            port = params['port']
            database = params['database']
            conn_args = params['conn_args'] if 'conn_args' in engine else None
            self.engines[engine] = EngineHandler(dialect=dialect,
                                                 driver=driver,
                                                 user=user,
                                                 pswd=pswd,
                                                 host=host,
                                                 port=port,
                                                 database=database,
                                                 conn_args=conn_args)

    def update_engines(self, creds):
        self.creds = creds
        create_new_engines = {}
        update_existing_engines = {}
        for engine, params in self.creds.items():
            if engine not in self.engines:
                create_new_engines[engine] = params
                if len(create_new_engines) > 0:
                    self._establish_engines(create_new_engines)
            else:
                update_existing_engines[engine] = params
                self.engines[engine].update_connection_parameters(dialect=params['dialect'],
                                                                  driver=params['driver'],
                                                                  user=params['user'],
                                                                  pswd=params['pswd'],
                                                                  host=params['host'],
                                                                  port=params['port'],
                                                                  database=params['database'],
                                                                  conn_args=params['conn_args']
                                                                  if 'conn_args' in params else None)

    def backup_db_table(self,
                        source_engine_to_backup,
                        backup_destination_engine,
                        backup_table):
        logging.info(f'Backing up the current state of the {backup_table} table.')
        post_meta_backup = pd.read_sql(f'select * from wp_liftenergypitt.{backup_table}',
                                       source_engine_to_backup).set_index('meta_id')
        read_timestamp = dt.datetime.now().strftime(DateTimes.date_string_format_text.value)
        logging.info(f'describing the table being backed-up, recorded at {read_timestamp}:\n'
                     f'{post_meta_backup.describe()}')
        src_url = str(source_engine_to_backup.url)
        dst_url = str(backup_destination_engine.url)

        backup_information_df = pd.read_sql('select * from db_table_backups', SqliteHandler().engine, index_col='index')
        logging.info(f'describing the backup information table df:\n'
                     f'{backup_information_df.describe()}')
        if backup_information_df.empty:
            backup_count = 0
        else:
            backup_count = backup_information_df.index.max()
        table_name = f'{backup_table}_backup_{backup_count + 1}'
        logging.info(f'evaluating table_name: {table_name}')
        backup_event = {'event_datetime': [read_timestamp],
                        'backup_source_engine': [src_url],
                        'backup_destination_engine': [dst_url]}
        new_budf = backup_information_df.append(pd.DataFrame(backup_event), ignore_index=True)
        logging.info(f'describing the new backup information table df after adding this backup event:\n'
                     f'{new_budf.describe()}')
        post_meta_backup.to_sql(table_name,
                                con=backup_destination_engine,
                                if_exists='fail')  # This means that the backup function has broke due to external factors.
        new_budf.to_sql('postmeta_backups', con=SqliteHandler().engine, if_exists='replace')
        return 'safe'

    def pivot_db_tables(self):
        """
        Just... It works
        This function will have it's parts parsed into a single line of functionality that can be utilized
        by parallel processing.
        """
        logging.info('Collecting all distinct post types.')
        ptqt = SQLText.distinct_post_types.value.text
        ptmcqt = SQLText.post_type_meta_collection_join.value.text
        post_type_df = pd.read_sql(ptqt, source_db_engine)
        type_list = [x[0] for x in post_type_df.values.tolist()]

        logging.info('Collecting all the columns for each post type.')
        type_dfs = {}
        type_key_dfs = {}
        type_value_dfs = {}
        for post_type in type_list:
            type_dfs[post_type] = pd.read_sql(ptmcqt % post_type, source_db_engine)
            if post_type not in type_key_dfs:
                type_key_dfs[post_type] = pd.read_sql(collect_meta_query_field_keys(post_type), source_db_engine)
            if post_type not in type_value_dfs:
                type_value_dfs[post_type] = pd.read_sql(collect_meta_query_field_values(post_type), source_db_engine)

        logging.info('Organizing all the columns into groups and fields for each post type.')
        type_organizations = {}
        for x in [type_key_dfs, type_value_dfs]:
            logging.debug(f'iterating through {x.keys()}')
            for pt, pt_df in x.items():
                logging.debug(f'evaluating {pt}\n\tpt_df: {pt_df.head(1)}')
                if pt not in type_organizations:
                    tmp_arg = pt_df['meta_key'].values.tolist()
                    logging.debug(f'passing tmp_arg to sift_metadata_to_groups method\n\ttmp_arg: {tmp_arg}')
                    pt_df_org = sift_metadata_to_groups(tmp_arg)
                    logging.debug(f'evaluating the return of the sifting method\n\t{pt_df_org}')
                    type_organizations[pt] = pt_df_org
                else:
                    tmp_arg = pt_df['meta_key'].values.tolist()
                    logging.debug(f'passing tmp_arg to sift_metadata_to_groups method\n\ttmp_arg: {tmp_arg}')
                    pt_df_org = sift_metadata_to_groups(tmp_arg)
                    logging.debug(f'evaluating the return of the sifting method\n\t{pt_df_org}')
                    type_organizations[pt].update(pt_df_org)

        logging.info(
            'Melting all the group/field combos with each post type into a type_group_table:columns dictionary')
        type_group_table_org = {}
        for pt, ptg in type_organizations.items():
            for group, cols in ptg.items():
                cols.append(group) if group != 'f' else None
                if group == 'f':
                    table_name = pt
                else:
                    table_name = f'{pt}_{group}'
                type_group_table_org[table_name] = cols

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

        logging.info('Merging the key and value dataframes together and placing them in a dictionary.')
        type_pivot_dfs = {}
        for tl in type_list:
            df_list = [x[tl] for x in [type_key_pivot_dfs, type_value_pivot_dfs] if tl in x]
            logging.debug(f'len(df_list):\n\t{len(df_list)}')
            if len(df_list) > 0:
                type_pivot_dfs[tl] = pd.concat(df_list, axis=1)

        logging.info('Parsing the merged dataframes into the type_group_table:columns format')
        pivot_tables = {}
        for table, columns in type_group_table_org.items():
            post_name = Regex.derive_post_from_table_name.value.search(table)
            logging.info(f'post_name: {post_name.group()} | table: {table} | len(columns): {len(columns)}')
            pivot_tables[table] = type_pivot_dfs[post_name.group()].loc[:, columns]

        logging.info('Pushing the type_group_table:columns dataframes into the pivot_tables schema.')
        for table_name, table_data in pivot_tables.items():
            logging.debug(f'dry run{table_name},\n\tcon={destination_db_engine}')
            table_data.to_sql(table_name,
                              con=destination_db_engine,
                              schema='postmeta_pivot_tables',
                              if_exists='replace')

        return [
            # No return value is required, however, these variables are returned for exploring the data in a console.
            post_type_df,
            type_list,
            type_key_dfs,
            type_value_dfs,
            type_value_pivot_dfs,
            type_key_pivot_dfs,
            type_organizations,
            type_group_table_org,
            type_pivot_dfs,
            pivot_tables
        ]

    def melt_pivot_tables(self):
        logging.info('Grabbing the list of pivot table names.')
        apt = SQLText.all_pivot_tables.value.text
        pt_df = pd.read_sql(apt, source_db_engine)
        table_names = [x[0] for x in pt_df.values.tolist()]

        logging.info('Extracting the contents of each pivot table.')
        pivot_dfs = [pd.read_sql(f'select * from pivot_tables.`{x}`', source_db_engine) for x in table_names]

        logging.info('Metling the extracted data.')
        melt_dfs = [df.melt(id_vars='post_id', var_name='meta_key', value_name='meta_value') for df in pivot_dfs]

        bu_status = backup_metadata(source_engine_to_backup=destination_db_engine,
                                    backup_destination_engine=backup_destination_engine)

        if bu_status == 'safe':
            logging.info('Updating the postmeta table with the melted data.')
            for df in melt_dfs:
                df.to_sql('wp_postmeta',
                          con=destination_db_engine,
                          schema='wp_liftenergypitt',
                          if_exists='append',
                          index=False)
        return [table_names, pivot_dfs, melt_dfs]

    def update_local_wp(self):
        """
        db sync will backup the contents of the destination engine.
        db sync will overwrite the contents of of the destination engine with contents of the source engine.

        """

        for table in sync_tables:
            table_df = pd.read_sql(f'select * from wp_liftenergypitt.{table}',
                                   pitt_db_engine)
            try:
                table_df.to_sql(table, local_db_engine, schema='wp_liftenergypitt', if_exists='replace', index=False)
            except Exception as e:
                logging.critical(e)
                logging.info(table)
                continue
        return 0

    @staticmethod
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

    @staticmethod
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

    @staticmethod
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
            logging.debug('Sift complete, returning the list\'s groups and their columns.')
            return group_dict


def main():
    creds_string = open('../secrets/creds.json', 'r').read()
    creds = json.loads(creds_string)
    logging.info(creds)
    dh = DataHandler(creds=creds)
    return dh


if __name__ == '__main__':
    data = main()
