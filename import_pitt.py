import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from modules.project_enums import Engines
from modules.project_enums import HandlerParams
from modules.project_enums import Messages
from modules.project_enums import SQLText

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


def parse_types():
    # get distinct post types
    #     generate a query for each post type
    #     return a df that needs to be pivoted.
    return 0


def pivot_attempt():
    engines = establish_engines()
    wp = engines[Engines.local_wp_engine_name.value]
    export_db = engines[Engines.sqlite_engine_name.value]
    qt = SQLText.test_pivot_original.value
    wpdf = pd.read_sql(qt, wp.engine, index_col='meta_id')
    wp_pivot = wpdf.pivot(index='post_id', columns='meta_key', values='meta_value')
    return {'original_df': wpdf, 'pivot_df': wp_pivot, 'export_db': export_db}


if __name__ == '__main__':
    setup = pivot_attempt()
    original = setup['original_df']
    piv = setup['pivot_df']
    export = setup['export_db']
    piv.to_sql(name='meta_pivot_values',
               con=export.engine,
               if_exists='replace',
               # schema='wp_liftenergypitt',
               index_label='post_id')
