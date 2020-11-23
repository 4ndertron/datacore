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

    def __init__(self, **kwargs):
        pm.logging.debug(f'running init of class Auditor in {__name__}')
        self.init_kwargs = kwargs
        self.init_kwarg_df = pm.pd.DataFrame({k: [v] for k, v in kwargs.items()})
        self.events = []
        self.credentials = kwargs.get('credentials')
        self.data_handler = pm.DataHandler(credentials=self.credentials.get('databases'))
        self.api_handler = pm.ApiHandler(credentials=self.credentials.get('requests'))
        self.default_pitt_engine = self.data_handler.engines['pitt_engine']
        self.default_loc_engine = self.data_handler.engines['docker_engine']
        self.default_lite_engine = self.data_handler.engines['sqlite_engine']
        self._setup_untracked_directories()

    def _setup_untracked_directories(self):
        """
        There are three directories used by this module that need to exists before any executions.
        This method will ensure those directories exists.
        """
        directories = {}
        if not pm.os.path.exists(pm.database_dir):
            pm.os.makedirs(pm.database_dir)
            directories[pm.database_dir] = True
        if not pm.os.path.exists(pm.secrets_dir):
            pm.os.makedirs(pm.secrets_dir)
            directories[pm.secrets_dir] = True
        if not pm.os.path.exists(pm.temp_dir):
            pm.os.makedirs(pm.temp_dir)
            directories[pm.secrets_dir] = True
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

    # noinspection PyTypeChecker
    def update_local_jn_db(self):
        """
        In order to use this method effectively, the following criteria must be met.
        1)  Export the contents of the report(s) "All Contact Columns" from JobNimbus as a CSV
        2)  Move the exported report file to the project's "temp" directory.

        After that, this method will read the contents of the report, figure out its entity type (contact, job, work order)
        and push that content into the Auditor's default local database environment.
        """
        pm.logging.debug(f'running update_local_jn_db in Auditor')
        csv_dir = pm.temp_dir
        files = pm.os.listdir(csv_dir)
        returns = {'exception': {
            'file': [],
            'e': [],
            'search': [],
            'entity': [],
            'df': [],
            'dfe': []
        }}
        for file in files:
            full_file_path = pm.os.path.join(csv_dir, file)
            search = pm.Regex.jn_export_entity.value.search(file)
            file_entity = self.format_file_name(search.groups()[0])
            file_df = pm.pd.read_csv(full_file_path, header=0, keep_default_na=False)
            try:
                db_df = pm.pd.read_sql(pm.SQLText.select_all_from_schema_table.value.text % ('jobnimbus', file_entity),
                                       self.default_loc_engine.engine)
                db_df.merge(file_df, how='outer', on='Id', copy=False)
                dfe = db_df.apply(  # text-encoded DataFrame
                    lambda x: x.apply(
                        lambda y: '' if y == '' else
                        pm.unicodedata.normalize('NFD', str(y)).encode('ascii', 'ignore')
                    )
                )
            except Exception as e:
                dfe = file_df.apply(  # text-encoded DataFrame
                    lambda x: x.apply(
                        lambda y: '' if y == '' else
                        pm.unicodedata.normalize('NFD', str(y)).encode('ascii', 'ignore')
                    )
                )
            try:
                pm.logging.debug(f'Pushing the text-encoded DataFrame derived from {file} to {file_entity} table'
                                 f'into {self.default_loc_engine.engine.url} database.')
                dfe.to_sql(file_entity,
                           self.default_loc_engine.engine,
                           schema='jobnimbus',
                           if_exists='replace',
                           index=False)
                if pm.os.path.exists(full_file_path):
                    pm.logging.debug(f'deleting {full_file_path}')
                    pm.os.remove(full_file_path)
            except Exception as e:
                returns['exception']['file'].append(file)
                returns['exception']['e'].append(e)
                returns['exception']['search'].append(search)
                returns['exception']['entity'].append(file_entity)
                returns['exception']['df'].append(file_df)
                returns['exception']['dfe'].append(dfe)
            returns[file] = {
                'split': pm.os.path.splitext(file),
                'df': file_df,
                'dfe': dfe,
                'entity': file_entity
            }
        return returns

    def run_auditor(self):
        d = self.update_local_jn_db()
        return {
            'self': self,
            'd': d,
        }
