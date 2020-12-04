import modules as pm  # pm being an acronym for project_modules


class Auditor:
    """
    What do I want to fix with this program?
        Address Validation Method
            1. Formatted Address Validation
            2. Acquire Address Lat
            3. Acquire Address Lng
        4. Fake Customer Accounts
        5. Missing Customer Data
        6. Easily crackable passwords
        7. Illegal field characters
    """
    TABLE_PREFIX = 'pd_'

    def __init__(self, **kwargs):
        pm.logging.debug(f'running init of class Auditor in {__name__}')
        self.init_kwargs = kwargs
        self.init_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.events = []
        self.credentials = kwargs.get('credentials')
        self.data_handler = pm.DataHandler(credentials=self.credentials.get('databases'))
        self.api_handler = pm.ApiHandler(credentials=self.credentials.get('requests'))
        self.gs_handler = pm.GSheetHandler(credentials=self.credentials.get('sheets'))
        self.default_dl_dir = pm.user_dl_dir
        self.default_pitt_engine = self.data_handler.engines.get('pitt_engine')
        self.default_loc_engine = self.data_handler.engines.get('docker_engine')
        self.default_pgo_engine = self.data_handler.engines.get('docker_pg_office')
        self.default_pgr_engine = self.data_handler.engines.get('docker_pg_remote')
        self.default_pg11_engine = self.data_handler.engines.get('docker_pg11')
        self.default_lite_engine = self.data_handler.engines.get('sqlite_engine')
        self.default_bucket_sheet = self.gs_handler.gsd.get('boards_workbook')
        self.default_status_change_sheet = self.gs_handler.gsd.get('status_changes')
        self.default_l10_sheet = self.gs_handler.gsd.get('corporate_l-10')
        self._setup_untracked_directories()

    def _setup_untracked_directories(self):
        """
        There are three directories used by this module that need to exists before any executions.
        This method will ensure those directories exists.
        """
        directories = {}
        for project_dir in (pm.database_dir, pm.secrets_dir, pm.temp_dir, pm.sql_dir):
            if not pm.os.path.exists(project_dir):
                pm.os.makedirs(project_dir)
            directories[project_dir] = True
        self.directories = directories

    def address_validation(self, **kwargs):
        """
        Function goals
            1. Convert an incomplete address into a completely formatted address
            2. Store the address' latitude coordinate
            3. Store the address' longitude coordinate
            4. Store the address' Google Place ID
        """
        stage = None
        if stage is not None:
            pm.logging.debug(f'running address_validation in Auditor')
            loc = self.data_handler.engines['docker_engine']

            jn_df = pm.pd.read_sql("select * from jobnimbus.contact", loc.engine)
            dft = jn_df.assign(account_id=lambda df: df.loc[:, 'Address Line']
                                                     + ', ' + df.loc[:, 'City']
                                                     + ', ' + df.loc[:, 'State']
                                                     + ', ' + df.loc[:, 'Zip']
                                                     + ', USA')

            test = {}
            loop_control = 0
            for row in dft.iterrows():
                loop_control += 1

                if row[1]['Address Line'] != '':
                    try:
                        test[row[1]['account_id']] = \
                            self.api_handler.return_search(row[1]['account_id'])['response'].json()['results'][0][
                                'geometry']['location']
                    except Exception as e:
                        test[row[1]['account_id']] = e

            search_returns = self.api_handler.return_search('576 East Deodar Lane, Lemoore, CA, , USA')
            r = search_returns['request']
            r_json = r.json()
            r_relevant_data = r_json['results'][0]
            r_address = r_relevant_data['formatted_address']
            r_geometry = r_relevant_data['geometry']['location']
            r_lat = r_geometry['lat']
            r_lng = r_geometry['lng']
            r_place_id = r_relevant_data['place_id']

            return 0

    def _validate_address(self, address):
        pm.logging.debug(f'running _validat_address in Auditor')

        return 0

    def create_new_records(self):
        pm.logging.debug(f'running create_new_records in Auditor')

        # pitt = dh.engines['pitt_engine']
        # loc = dh.engines['docker_engine']
        # lite = dh.engines['sqlite_engine']

        # convert_returns = dh.convert_jn_tables_to_wp(jn_engine=loc, tp_engine=pitt, ld_engine=loc, field_map=jn_map)
        # bridge = convert_returns[3]
        # users_dict = convert_returns[4]

        # for i in range(3):  # This works
        #     account_post = dh.create_single_post_df(post_type='account', creator_id=5, source_engine=loc)
        #     account_post.to_sql('wp_posts', loc.engine, schema='wp_liftenergypitt', if_exists='append', index=False)

        return 0

    @staticmethod
    def format_file_name(string):
        pm.logging.debug(f'running format_file_name in Auditor')
        formatted_string = pm.re.sub(' ', '_', string.lower())
        return formatted_string

    def export_local_jn_db(self):
        pm.logging.debug(f'running export_local_jn_db in Auditor')
        return 0

    # noinspection PyTypeChecker
    def update_local_jn_db(self, **kwargs):
        """
        In order to use this method effectively, the following criteria must be met.
        1)  Export the contents of the report(s) "All Contact Columns" from JobNimbus as a CSV
        2)  Move the exported report file to the project's "temp" directory.

        After that, this method will read the contents of the report, figure out its entity type (contact, job, work order)
        and push that content into the Auditor's default local database environment.
        """
        pm.logging.debug(f'running update_local_jn_db in Auditor')
        update_engine = kwargs.get('update_engine') if kwargs.get(
            'update_engine') is not None else self.default_pgo_engine
        csv_dir = pm.temp_dir
        pm.logging.debug(f'looking for jn files in {csv_dir}')
        files = pm.os.listdir(csv_dir)
        returns = {'exception': {
            'file': [],
            'e': [],
            'search': [],
            'entity': [],
            'df': [],
            'db_df': [],
            'dfe': [],
        }}
        for file in files:
            full_file_path = pm.os.path.join(csv_dir, file)

            search = pm.Regex.jn_export_entity.value.search(file)
            file_entity = self.format_file_name(search.groups()[0])

            default_schema = 'public'
            table_name = f'{self.TABLE_PREFIX}{file_entity}'
            query_tuple = (default_schema, table_name)

            conversion_map = pm.DfColumnConversion.loop_entities.value[file_entity]

            file_df = pm.pd.read_csv(full_file_path, header=0, keep_default_na=False)

            pm.logging.debug(f'converting {file_entity} df with map: {conversion_map}')
            file_df_convert = file_df.astype(pm.DfColumnConversion.loop_entities.value[file_entity])

            try:
                db_df = pm.pd.read_sql(pm.SQLText.select_all_from_schema_table.value.text % query_tuple,
                                       update_engine.engine)
                mdf = db_df.merge(file_df_convert, how='outer')
                dfe = mdf
                # dfe = mdf.apply(  # encode the text-type cells
                #     lambda x:
                #     x.apply(
                #         lambda y:
                #         pm.logging.debug(f'type(y) in col {x.title}:\n{type(y)}') if type(y) != pm.np.str else
                #         pm.unicodedata.normalize('NFD', str(y)).encode('ascii', 'ignore')
                #     )str
                # )
            except Exception as e:
                pm.logging.debug(e)
                db_df = None
                dfe = file_df_convert
                # dfe = file_df.apply(  # encode the text-type cells
                #     lambda x:
                #     x.apply(
                #         lambda y:
                #         pm.logging.debug(f'type(y) in col empy:\n{type(y)}') if type(y) != pm.np.str else
                #         pm.unicodedata.normalize('NFD', str(y)).encode('ascii', 'ignore')
                #     )
                # )
            try:
                pm.logging.info(f'Pushing the text-encoded DataFrame derived from {file} to {file_entity} table '
                                f'in {update_engine.engine.url} database.')
                dfe.drop_duplicates(inplace=True)
                pm.logging.info(f'dropped duplicates for {file_entity} dfe')
                pm.logging.debug(f'pushing {table_name} to {default_schema}')
                dfe.to_sql(table_name,
                           update_engine.engine,
                           schema=default_schema,
                           if_exists='replace',
                           index=False)
                pm.logging.info(f'to_sql for active dfe has executed to {file_entity} from {full_file_path}')
                if pm.os.path.exists(full_file_path):
                    pm.logging.debug(f'deleting {full_file_path}')
                    pm.os.remove(full_file_path)
            except Exception as e:
                returns['exception']['file'].append(file)
                returns['exception']['e'].append(e)
                returns['exception']['search'].append(search)
                returns['exception']['entity'].append(file_entity)
                returns['exception']['df'].append(file_df_convert)
                returns['exception']['db_df'].append(db_df)
                returns['exception']['dfe'].append(dfe)
            returns[file] = {
                'split': pm.os.path.splitext(file),
                'df': file_df_convert,
                'db_df': db_df,
                'dfe': dfe,
                'entity': file_entity,
            }
        return returns

    def populate_window_metrics(self, **kwargs):
        """
        Calculation Workflow:
        1. Pull change logs to local env
        2. push change logs to db
        3. have sql calculate the "actual" metric.
        4. Have loc env pull calculated actuals.
        5. have loc env push actuals to this column.
        """
        pm.logging.debug(f'running populate_window_metrics in Auditor')
        update_engine = kwargs.get('update_engine') if kwargs.get(
            'update_engine') is not None else self.default_pgo_engine
        if update_engine is None:
            update_engine = self.default_pgo_engine
        changes = self.pull_gs_data(gs_range="Test!D:G",
                                    sheet=self.default_status_change_sheet,
                                    table='status_changes',
                                    engine=update_engine.engine)
        windows = self.pull_gs_data(gs_range="L10!C:J",
                                    sheet=self.default_status_change_sheet,
                                    table='status_windows',
                                    engine=update_engine.engine)
        boards = self.pull_gs_data(gs_range="Union!A:F",
                                   sheet=self.default_bucket_sheet,
                                   table='board',
                                   engine=update_engine.engine)
        gs_pushes = {'status_changes': changes, 'status_windows': windows, 'board': boards}
        for tn, df in gs_pushes.items():
            df.to_sql(f'{self.TABLE_PREFIX}{tn}',
                      update_engine.engine,
                      schema='public',
                      index=False,
                      if_exists='replace')
        # response = self.default_loc_engine.run_query_file()
        # metrics = pm.pd.read_sql('select * from metrics.table', self.default_loc_engine, index_col='window_id')
        # actuals = metrics.loc[:, 'actual']
        # self.default_status_change_sheet.update_range(range='L10!I2:I',
        #                                               data=actuals,
        #                                               sheet=self.default_status_change_sheet)
        return gs_pushes

    def pull_gs_data(self, **kwargs):
        pm.logging.debug(f'running pull_gs_data in Auditor')
        gs_range = kwargs.get('gs_range')
        sheet = kwargs.get('sheet')
        table = kwargs.get('table')
        engine = kwargs.get('engine')
        sheet_values = sheet.gather_range_values(gs_range)
        new_gs_df = pm.pd.DataFrame(sheet_values[1:], columns=sheet_values[:1][0])
        try:
            old_gs_df = pm.pd.read_sql(pm.SQLText.select_all_from_schema_table.value.text % ('public', table),
                                       engine)
            return_df = old_gs_df.merge(new_gs_df, how='outer')
        except Exception as e:
            pm.logging.debug(e)
            return_df = new_gs_df
        return return_df.drop_duplicates(ignore_index=True)

    def run_auditor(self, **kwargs):
        use_engine = kwargs.get('use_engine') if kwargs.get('use_engine') is not None else self.default_pgo_engine
        d = self.update_local_jn_db(update_engine=use_engine)
        wm = self.populate_window_metrics(update_engine=use_engine)
        return {
            'self': self,
            'd': d,
            'wm': wm,
        }
