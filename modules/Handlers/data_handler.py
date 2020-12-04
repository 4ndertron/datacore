import modules as pm  # pm being an acronym for project_modules
from .core_units.sql_engine_core import SqlEngineCore


class DataHandler:
    """
    Database order of operations:
    1)  Pull
        - This step includes pulling the existing state of the database into memory.
    2)  Place
        - This step appends the two datasets together.
    3)  Purge
        - This step removes duplicate rows.
    4)  Push
        - This step replaces the old state of the database with the new dataset.
    """

    def __init__(self, *args, **kwargs):
        pm.logging.debug(f'running init of class DataHandler in {__name__}')
        self.init_kwargs = kwargs
        self.init_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.engines = {}
        self.cred = kwargs.get('credentials')
        self._establish_engines()
        self.primary_backup_engine = SqlEngineCore(dialect='sqlite',
                                                   database=pm.os.path.join(pm.database_dir, 'foo.db'))

    def _establish_engines(self, creds=None):
        pm.logging.debug(f'running _establish_engines in DataHandler')
        if creds is None:
            creds = self.cred
        for engine, params in creds.items():
            dialect = params.get('dialect')
            driver = params.get('driver')
            user = params.get('user')
            pswd = params.get('pswd')
            host = params.get('host')
            port = params.get('port')
            database = params.get('database')
            conn_args = params.get('conn_args')
            self.engines[engine] = SqlEngineCore(dialect=dialect,
                                                 driver=driver,
                                                 user=user,
                                                 pswd=pswd,
                                                 host=host,
                                                 port=port,
                                                 database=database,
                                                 conn_args=conn_args)

    def update_engines(self, creds):
        pm.logging.debug(f'running _establish_engines in DataHandler')
        self.cred = creds
        create_new_engines = {}
        update_existing_engines = {}
        for engine, params in self.cred.items():
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
                                                                  database=params['data'],
                                                                  conn_args=params['conn_args']
                                                                  if 'conn_args' in params else None)

    def backup_db_table(self,
                        source_engine_to_backup=None,
                        backup_destination_engine=None,
                        backup_table=None):
        pm.logging.debug(f'running backup_db_table in DataHandler')
        if backup_destination_engine is None:
            backup_destination_engine = self.primary_backup_engine
        pm.logging.info(f'Backing up the current state of the {backup_table} table.')
        post_meta_backup = pm.pd.read_sql(pm.SQLText.select_all_from_table.value.text % backup_table,
                                          source_engine_to_backup.engine)
        read_timestamp = pm.dt.datetime.now().strftime(pm.DateTimes.date_string_format_text.value)
        pm.logging.debug(f'describing the table {backup_table}, recorded at {read_timestamp}:\n'
                         f'{post_meta_backup.describe()}')
        source_url = str(source_engine_to_backup.engine.url)
        destination_url = str(backup_destination_engine.engine.url)

        backup_information_df = pm.pd.read_sql(pm.SQLText.select_backup_tables.value.text,
                                               backup_destination_engine.engine,
                                               index_col='index')
        pm.logging.debug(f'describing the {backup_table} table df:\n'
                         f'{backup_information_df.describe()}')
        if backup_information_df.empty:
            table_count = 0
        else:
            table_count = backup_information_df['table_count'] \
                .where(backup_information_df['backup_table'] == backup_table).count()
        table_name = f'{backup_table}_backup_{table_count + 1}'
        pm.logging.info(f'evaluating table_name: {table_name}')
        backup_event = {'event_datetime': [read_timestamp],
                        'backup_source_engine': [source_url],
                        'backup_destination_engine': [destination_url],
                        'backup_table': [backup_table],
                        'table_count': [table_count + 1],
                        'backup_table_name': [f'{backup_table}_backup_{table_count + 1}']}
        new_budf = backup_information_df.append(pm.pd.DataFrame(backup_event), ignore_index=True)
        pm.logging.debug(f'describing the new backup information table df after adding this backup event:\n'
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
        pm.logging.debug(f'running pivot_db_tables in DataHandler')
        returns = {}
        if source_db_engine is None:
            source_db_engine = self.engines['pitt_engine'].engine
        if destination_db_engine is None:
            destination_db_engine = self.engines['docker_engine'].engine
        if table_list is None or len(table_list) < 1:
            return pm.Messages.no_tables.value
        for table in table_list:
            if 'meta' in table:
                pm.logging.info(f'Begining the pivot procedure for {table}')
                returns[table] = self._pivot_db_table(source_db_engine=source_db_engine,
                                                      destination_db_engine=destination_db_engine,
                                                      source_table=table)
            else:
                return pm.Messages.invalid_table_type.value % table
        return returns

    def _pivot_db_table(self,
                        source_db_engine,
                        destination_db_engine,
                        source_table):
        pm.logging.debug(f'running _pivot_db_table in DataHandler')
        pm.logging.info(f'Collecting all distinct post types for {source_table}.')

        table_prefix = pm.Regex.wpengine_table_prefix.value
        table_suffix = pm.Regex.wpengine_meta_suffix.value
        table_entity = table_prefix.sub("", table_suffix.sub("", source_table))

        meta_key_query_text = pm.SQLText.select_distinct_meta_keys.value.text % source_table
        meta_key_df = pm.pd.read_sql(meta_key_query_text, source_db_engine.engine)
        meta_keys = [x[0] for x in meta_key_df.values.tolist()]
        meta_keys_split = [[], []]
        for key in meta_keys:
            index = 0 if key.startswith('_') else 1
            meta_keys_split[index].append(key)
        meta_keys_org = [self.sift_metadata_to_groups(split) for split in meta_keys_split]

        table_content_qt = pm.SQLText.select_all_from_table.value.text % source_table

        meta_df = pm.pd.read_sql(table_content_qt, source_db_engine.engine)
        pivot_index = f'{table_entity}_id'
        meta_pivot_df = meta_df.pivot(index=pivot_index, columns='meta_key', values='meta_value')

        pivot_dfs = {}
        for org in meta_keys_org:
            for table, columns in org.items():
                pivot_dfs[table] = meta_pivot_df.loc[:, columns]

        schema_name = f'{source_table}_pivot'
        destination_db_engine.create_schema(schema_name=schema_name)
        for table_name, table_data in pivot_dfs.items():
            pm.logging.debug(f'dry run {table_name}\ncon={destination_db_engine}\nschema={schema_name}')
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
        pm.logging.debug(f'running melt_pivot_schemas in DataHandler')
        if source_db_engine is None:
            source_db_engine = self.engines['docker_engine']
        if destination_db_engine is None:
            destination_db_engine = self.engines['docker_engine']
        if backup_engine is None:
            backup_engine = self.engines['sqlite_engine']
        if schema_list is None:
            return pm.Messages.no_schemas.value
        for schema in schema_list:
            if 'pivot' in schema:
                self._melt_pivot_tables(source_db_engine=source_db_engine,
                                        destination_db_engine=destination_db_engine,
                                        backup_engine=backup_engine,
                                        schema_name=schema)
            else:
                return pm.Messages.invalid_schema_type.value % schema

    def _melt_pivot_tables(self,
                           source_db_engine,
                           destination_db_engine,
                           backup_engine,
                           schema_name):
        pm.logging.debug(f'running _melt_pivot_tables in DataHandler')
        pm.logging.info(f'Grabbing the list of pivot table names from {schema_name}.')
        sst = pm.SQLText.select_schema_tables.value.text % f'\'{schema_name}\''
        schema_prefix = pm.Regex.wpengine_table_prefix.value
        schema_suffix = pm.Regex.wpengine_meta_suffix.value
        pivot_suffix = pm.Regex.pivot_schema_suffix.value
        melt_id = f'{schema_prefix.sub("", schema_suffix.sub("", pivot_suffix.sub("", schema_name)))}_id'
        destination_table = pivot_suffix.sub("", schema_name)
        schema_tables = pm.pd.read_sql(sst, source_db_engine.engine)
        table_names = [x[0] for x in schema_tables.values.tolist()]

        pm.logging.info('Extracting the contents of each pivot table.')
        pivot_dfs = [pm.pd.read_sql(f'select * from {schema_name}.`{x}`', source_db_engine.engine) for x in table_names]

        pm.logging.info('Metling the extracted data.')
        melt_dfs = [df.melt(id_vars=melt_id, var_name='meta_key', value_name='meta_value') for df in pivot_dfs]

        bu_status = self.backup_db_table(source_engine_to_backup=destination_db_engine,
                                         backup_destination_engine=backup_engine,
                                         backup_table=destination_table)
        if bu_status == 'safe':
            pm.logging.info('Updating the postmeta table with the melted data.')

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
        pm.logging.debug(f'running update_local_wp in DataHandler')
        pitt_engine = self.engines['pitt_engine'].engine
        local_wp_engine = self.engines['docker_engine'].engine
        returns = {'success': {}, 'failures': {}}
        if sync_tables is None:
            tables_df = pm.pd.read_sql(
                pm.SQLText.select_schema_tables.value.text % '\'wp_liftenergypitt\'',
                pitt_engine)
            sync_tables = [x[0] for x in tables_df.values.tolist()]
        for table in sync_tables:
            broken_table_df = pm.pd.read_sql(pm.SQLText.select_all_from_table.value.text % table,
                                             pitt_engine)
            pm.logging.debug(f'beginning the attempted normalization of {table}')
            table_df = broken_table_df.apply(
                lambda x: x.apply(lambda y: pm.unicodedata.normalize('NFD', str(y)).encode('ascii', 'ignore')))
            try:
                if table not in ('wp_options', 'wp_wfconfig', 'wp_wfwafconfig'):
                    pm.logging.debug(f'attempting to update {table} with normalized data.')
                    table_df.to_sql(table, local_wp_engine, schema='wp_liftenergypitt', if_exists='replace',
                                    index=False)
                    pm.logging.debug(f'Attempt at updating {table} succeeded.')
                    returns['success'][table] = table_df
            except Exception as e:
                pm.logging.critical(e)
                pm.logging.info(table)
                returns['failures'][table] = {'event': e, 'df': table_df}
                continue
        return returns

    def convert_jn_tables_to_wp(self, **kwargs):
        """
        potential workflow:
        collect active users
        for each user:
            gather all accounts
            create a blank account for the number of accounts.
            for each account:
                get post_id
                populate post_id with account_id
                use account/post_id pairs to map jn and wp tables together in the sequence of the pitt's workflow
        """
        pm.logging.debug(f'running convert_jn_tables_to_wp in DataHandler')
        tp_engine = kwargs.get('tp_engine')
        ld_engine = kwargs.get('ld_engine')
        jn_engine = kwargs.get('jn_engine')
        pm.logging.debug('tp_users pre-definition')
        tp_users = pm.pd.read_sql(pm.SQLText.select_all_from_table.value.text % 'wp_users', tp_engine.engine)
        field_mapping = kwargs.get('field_map')
        pm.logging.debug('account_ids pre-definition')
        account_ids = pm.pd.read_sql(pm.SQLText.select_distinct_jobnimbus_accounts.value.text, jn_engine.engine)
        pm.logging.debug('jn_tables pre-definition')
        jn_tables = pm.pd.read_sql("select table_name from information_schema.tables where TABLE_SCHEMA = 'jobnimbus'",
                                   jn_engine.engine)

        pm.logging.debug('jn_df pre-definition')
        jn_df = pm.pd.read_sql("select * from jobnimbus.contact", jn_engine.engine)
        pm.logging.debug('jn_dft pre-definition')
        jn_dft = jn_df.assign(account_id=lambda df: df.loc[:, 'Address Line']
                                                    + ', ' + df.loc[:, 'City']
                                                    + ', ' + df.loc[:, 'State']
                                                    + ', ' + df.loc[:, 'Zip']
                                                    + ', USA')
        pm.logging.debug('column_bridge pre-definition')
        column_bridge = pm.pd.read_sql(pm.SQLText.select_pivot_column_metadata.value.text, jn_engine.engine)
        pm.logging.debug('jn_tables_dict pre-definition')
        jn_tables_dict = jn_tables.to_dict()

        pm.logging.debug('users_dfs pre-definition')
        users_dfs = {}
        pm.logging.debug('users_dfs pre-loop')
        for row in tp_users.iterrows():
            pm.logging.debug(f'users_dfs in-loop, row {row}')
            users_dfs[row[1]['ID']] = jn_dft.loc[jn_dft.loc[:, 'Sales Rep'] == row[1]['display_name'], :]
        self._test_users_dict(users_dict=users_dfs)

        pm.logging.debug('convert_jn_tables_to_wp pre-return')
        return [
            account_ids,  # 0
            jn_tables,  # 1
            jn_df,  # 2
            column_bridge,  # 3
            users_dfs  # 4
        ]

    def _test_users_dict(self, **kwargs):
        pm.logging.debug(f'running _test_uses_dict in DataHandler')
        users = kwargs.get('users_dict')
        for user, df in users.items():
            if df.empty:
                continue
            print(f'{user} len(df): {len(df)}')

    def create_single_post_df(self, **kwargs):
        pm.logging.debug(f'running create_single_post_df in DataHandler')
        post_type = kwargs.get('post_type')
        creator_id = kwargs.get('creator_id')
        source_engine = kwargs.get('source_engine')
        pm.logging.debug(kwargs)

        timestamp = pm.dt.datetime.now().strftime(pm.DateTimes.date_string_format_text.value)
        # todo: factor in thepitt.io's global counter... later
        #   it may not be required since this program is usually for a one time update of old records.
        post_title = self._calculate_post_type_title(post_type=post_type, source_engine=source_engine)
        guid = f'https://thpitt.io/{post_type}/{post_title}/'

        pm.logging.debug(post_title)

        post_type_template = pm.JobNimbusToWPEngineMapping.conversion_map.value[post_type]['post']
        post_type_template['post_type'] = post_type
        post_type_template['post_author'] = creator_id
        post_type_template['post_title'] = post_title
        post_type_template['post_name'] = post_title
        post_type_template['guid'] = guid
        post_type_template['post_date'] = timestamp
        post_type_template['post_date_gmt'] = timestamp
        post_type_template['post_modified'] = timestamp
        post_type_template['post_modified_gmt'] = timestamp
        return pm.pd.DataFrame(post_type_template)

    def _calculate_post_type_title(self, **kwargs):
        pm.logging.debug(f'running _calculate_post_type_title in DataHandler')
        post_type = kwargs.get('post_type')
        source_engine = kwargs.get('source_engine')
        if source_engine is None:
            source_engine = self.engines['docker_engine']
        qt = pm.SQLText.select_account_tally.value.text % post_type
        pm.logging.debug(qt)
        current_max_tally = pm.pd.read_sql(qt, source_engine.engine)
        return int(current_max_tally.to_dict()['idcount'][0]) + 1

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
        pm.logging.debug(f'running collect_meta_query_field_values in DataHandler')
        raw_query_text = pm.SQLText.post_type_meta_collection_split.value.text
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
        pm.logging.debug(f'running collect_meta_query_field_keys in DataHandler')
        raw_query_text = pm.SQLText.post_type_meta_collection_split.value.text
        return raw_query_text % ('like \'\_%\'', post_type)

    @staticmethod
    def sift_metadata_to_groups(name_list=None, group_dict=None):
        pm.logging.debug(f'running sift_metadata_to_groups in DataHandler')
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
            pm.logging.debug('Sifting the name list provided.')
            labels = {}

            pm.logging.debug('Determining which indexes in the name list is a field and which is a group.')
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

            pm.logging.debug('Matching the parent group to each field.')
            for group, glab in groups.items():
                for field, flab in fields.items():
                    if group in field and len(group) > len(flab):
                        fields[field] = group

            pm.logging.debug('Generating a dictionary of group names and a list of their columns.')
            for field, parent in fields.items():
                if parent not in group_dict:
                    group_dict[parent] = [field]
                else:
                    group_dict[parent].append(field)
            pm.logging.debug('Sift complete, returning the list\'s groups and their columns.')
            return group_dict
