from modules import dt
from modules import pd
from modules import json
from modules import logging
from modules.sql_engines import EngineHandler
from modules.project_enums import Regex
from modules.project_enums import SQLText
from modules.project_enums import Messages
from modules.project_enums import DateTimes


class DataHandler:
    def __init__(self, *args, **kwargs):
        self.engines = {}
        self.creds = kwargs['creds'] if 'creds' in kwargs else None
        self.primary_backup_engine = EngineHandler(dialect='sqlite', database='./data/foo.db')
        self._establish_engines()

    def _establish_engines(self, creds=None):
        if creds is None:
            creds = self.creds
        for engine, params in creds.items():
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
                        source_engine_to_backup=None,
                        backup_destination_engine=None,
                        backup_table=None):
        if backup_destination_engine is None:
            backup_destination_engine = self.primary_backup_engine
        logging.info(f'Backing up the current state of the {backup_table} table.')
        post_meta_backup = pd.read_sql(SQLText.select_all_from_table.value.text % backup_table,
                                       source_engine_to_backup.engine)
        read_timestamp = dt.datetime.now().strftime(DateTimes.date_string_format_text.value)
        logging.debug(f'describing the table {backup_table}, recorded at {read_timestamp}:\n'
                      f'{post_meta_backup.describe()}')
        source_url = str(source_engine_to_backup.engine.url)
        destination_url = str(backup_destination_engine.engine.url)

        backup_information_df = pd.read_sql(SQLText.select_backup_tables.value.text,
                                            backup_destination_engine.engine,
                                            index_col='index')
        logging.debug(f'describing the {backup_table} table df:\n'
                      f'{backup_information_df.describe()}')
        if backup_information_df.empty:
            table_count = 0
        else:
            table_count = backup_information_df['table_count'] \
                .where(backup_information_df['backup_table'] == backup_table).count()
        table_name = f'{backup_table}_backup_{table_count + 1}'
        logging.info(f'evaluating table_name: {table_name}')
        backup_event = {'event_datetime': [read_timestamp],
                        'backup_source_engine': [source_url],
                        'backup_destination_engine': [destination_url],
                        'backup_table': [backup_table],
                        'table_count': [table_count + 1],
                        'backup_table_name': [f'{backup_table}_backup_{table_count + 1}']}
        new_budf = backup_information_df.append(pd.DataFrame(backup_event), ignore_index=True)
        logging.debug(f'describing the new backup information table df after adding this backup event:\n'
                      f'{new_budf.describe()}')
        post_meta_backup.to_sql(table_name,
                                con=backup_destination_engine.engine,
                                index=False,
                                if_exists='fail')  # This prevents failures due to external factors.
        new_budf.to_sql('db_table_backups', con=backup_destination_engine.engine, if_exists='replace')
        return 'safe'

    def pivot_db_tables(self,
                        source_db_engine=None,
                        destination_db_engine=None,
                        table_list=None):
        returns = {}
        if source_db_engine is None:
            source_db_engine = self.engines['pitt_engine'].engine
        if destination_db_engine is None:
            destination_db_engine = self.engines['local_wp_engine'].engine
        if table_list is None or len(table_list) < 1:
            return Messages.no_tables.value
        for table in table_list:
            if 'meta' in table:
                logging.info(f'Begining the pivot procedure for {table}')
                returns[table] = self._pivot_db_table(source_db_engine=source_db_engine,
                                                      destination_db_engine=destination_db_engine,
                                                      source_table=table)
            else:
                return Messages.invalid_table_type.value % table
        return returns

    def _pivot_db_table(self,
                        source_db_engine,
                        destination_db_engine,
                        source_table):
        logging.info(f'Collecting all distinct post types for {source_table}.')

        table_prefix = Regex.wpengine_table_prefix.value
        table_suffix = Regex.wpengine_meta_suffix.value
        table_entity = table_prefix.sub("", table_suffix.sub("", source_table))

        meta_key_query_text = SQLText.select_distinct_meta_keys.value.text % source_table
        meta_key_df = pd.read_sql(meta_key_query_text, source_db_engine.engine)
        meta_keys = [x[0] for x in meta_key_df.values.tolist()]
        meta_keys_split = [[], []]
        for key in meta_keys:
            index = 0 if key.startswith('_') else 1
            meta_keys_split[index].append(key)
        meta_keys_org = [self.sift_metadata_to_groups(split) for split in meta_keys_split]

        table_content_qt = SQLText.select_all_from_table.value.text % source_table

        meta_df = pd.read_sql(table_content_qt, source_db_engine.engine)
        pivot_index = f'{table_entity}_id'
        meta_pivot_df = meta_df.pivot(index=pivot_index, columns='meta_key', values='meta_value')

        pivot_dfs = {}
        for org in meta_keys_org:
            for table, columns in org.items():
                pivot_dfs[table] = meta_pivot_df.loc[:, columns]

        schema_name = f'{source_table}_pivot'
        destination_db_engine.create_schema(schema_name=schema_name)
        for table_name, table_data in pivot_dfs.items():
            logging.debug(f'dry run {table_name}\ncon={destination_db_engine}\nschema={schema_name}')
            table_data.dropna(how='all', inplace=True, thresh=1)
            table_data.to_sql(table_name,
                              con=destination_db_engine.engine,
                              schema=schema_name,
                              if_exists='replace')

        return [meta_keys,
                meta_keys_org,
                meta_df,
                meta_pivot_df,
                pivot_dfs,
                schema_name]

    def melt_pivot_schemas(self,
                           source_db_engine=None,
                           destination_db_engine=None,
                           backup_engine=None,
                           schema_list=None):
        if source_db_engine is None:
            source_db_engine = self.engines['local_wp_engine']
        if destination_db_engine is None:
            destination_db_engine = self.engines['local_wp_engine']
        if backup_engine is None:
            backup_engine = self.engines['sqlite_engine']
        if schema_list is None:
            return Messages.no_schemas.value
        for schema in schema_list:
            if 'pivot' in schema:
                self._melt_pivot_tables(source_db_engine=source_db_engine,
                                        destination_db_engine=destination_db_engine,
                                        backup_engine=backup_engine,
                                        schema_name=schema)
            else:
                return Messages.invalid_schema_type.value % schema

    def _melt_pivot_tables(self,
                           source_db_engine,
                           destination_db_engine,
                           backup_engine,
                           schema_name):
        logging.info(f'Grabbing the list of pivot table names from {schema_name}.')
        sst = SQLText.select_schema_tables.value.text % f'\'{schema_name}\''
        schema_prefix = Regex.wpengine_table_prefix.value
        schema_suffix = Regex.wpengine_meta_suffix.value
        pivot_suffix = Regex.pivot_schema_suffix.value
        melt_id = f'{schema_prefix.sub("", schema_suffix.sub("", pivot_suffix.sub("", schema_name)))}_id'
        destination_table = pivot_suffix.sub("", schema_name)
        schema_tables = pd.read_sql(sst, source_db_engine.engine)
        table_names = [x[0] for x in schema_tables.values.tolist()]

        logging.info('Extracting the contents of each pivot table.')
        pivot_dfs = [pd.read_sql(f'select * from {schema_name}.`{x}`', source_db_engine.engine) for x in table_names]

        logging.info('Metling the extracted data.')
        melt_dfs = [df.melt(id_vars=melt_id, var_name='meta_key', value_name='meta_value') for df in pivot_dfs]

        bu_status = self.backup_db_table(source_engine_to_backup=destination_db_engine,
                                         backup_destination_engine=backup_engine,
                                         backup_table=destination_table)
        if bu_status == 'safe':
            logging.info('Updating the postmeta table with the melted data.')

            for df in melt_dfs:
                df.to_sql(destination_table,
                          con=destination_db_engine.engine,
                          schema='wp_liftenergypitt',
                          if_exists='append',
                          index=False)

        return [table_names, pivot_dfs, melt_dfs]

    def update_local_wp(self, sync_tables=None):
        """
        db sync will backup the contents of the destination engine.
        db sync will overwrite the contents of of the destination engine with contents of the source engine.

        """
        pitt_engine = self.engines['pitt_engine'].engine
        local_wp_engine = self.engines['local_wp_engine'].engine
        returns = {'success': {}, 'failures': {}}
        if sync_tables is None:
            tables_df = pd.read_sql(
                SQLText.select_schema_tables.value.text % '\'wp_liftenergypitt\' and table_name not like \'%wf%\'',
                pitt_engine)
            sync_tables = [x[0] for x in tables_df.values.tolist()]
        for table in sync_tables:
            table_df = pd.read_sql(SQLText.select_all_from_table.value.text % table,
                                   pitt_engine)
            try:
                if table != 'wp_options':
                    table_df.to_sql(table, local_wp_engine, schema='wp_liftenergypitt', if_exists='replace',
                                    index=False)
                    returns['success'][table] = table_df
            except Exception as e:
                logging.critical(e)
                logging.info(table)
                returns['failures'][table] = {'event': e, 'df': table_df}
                continue
        return returns

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
    data_handler = DataHandler(creds=creds)
    return data_handler


if __name__ == '__main__':
    dh = main()
    pitt = dh.engines['pitt_engine']
    loc = dh.engines['local_wp_engine']
    lite = dh.engines['sqlite_engine']

    pivot_tables = ['wp_postmeta', 'wp_usermeta']
    melt_schemas = ['wp_postmeta_pivot', 'wp_usermeta_pivot']

    pivot_returns = dh.pivot_db_tables(source_db_engine=pitt,
                                       destination_db_engine=loc,
                                       table_list=pivot_tables)

    melt_returns = dh.melt_pivot_schemas(source_db_engine=loc,
                                         destination_db_engine=loc,
                                         backup_engine=lite,
                                         schema_list=melt_schemas)

    update_return = dh.update_local_wp()
