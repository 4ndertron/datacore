from sqlalchemy import create_engine
from modules.module_enums import Engines
from modules.module_enums import Messages
from modules.module_enums import SQLText
from modules.module_enums import HandlerParams as hp


class EngineHandler:
    def __init__(self, *args, **kwargs):
        self.valid_parameters = hp.valid_params.value
        self.dialect = kwargs.get('dialect')
        self.driver = kwargs.get('driver')
        self.user = kwargs.get('user')
        self.pswd = kwargs.get('pswd')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.database = kwargs.get('database')
        self.conn_args = kwargs.get('conn_args')
        self.engine = None
        self.conn = None
        self._setup_engine()

    def _setup_engine(self):
        if self.dialect == 'sqlite':
            url = f'{self.dialect}:///{self.database}'
        else:
            url = f'{self.dialect}{self.driver}://{self.user}:{self.pswd}@' \
                  f'{self.host}:{self.port}{self.database}'
        if self.conn_args is None:
            self.engine = create_engine(url)
        else:
            self.engine = create_engine(url, connect_args=self.conn_args)

    def _create_conn(self):
        self.conn = self.engine.connect()

    def _close_conn(self):
        self.conn.close()
        self.conn = None

    def update_connection_parameters(self, **kwargs):
        self.dialect = kwargs.get('dialect')
        self.driver = kwargs.get('driver')
        self.user = kwargs.get('user')
        self.pswd = kwargs.get('pswd')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.database = kwargs.get('database')
        self.conn_args = kwargs.get('conn_args')
        returns = []
        for k, v in kwargs.items():
            if k in self.valid_parameters:
                returns.append(k)
        if len(returns) == 0:
            return Messages.no_valid_parameters.value
        else:
            self._setup_engine()
            return f'{Messages.updated_valid_parameters.value}{",".join(returns)}'

    def schema_exists(self, schema_name):
        self._create_conn()
        schemas = [x[1] for x in self.conn.execute(SQLText.select_schemas.value).fetchall()]
        self._close_conn()
        return schema_name in schemas

    def create_schema(self, schema_name):
        if self.schema_exists(schema_name):
            return f'{schema_name} exists'
        else:
            self._create_conn()
            self.conn.execute(SQLText.create_schema_sql.value.text % schema_name)
            self._close_conn()
            return f'{schema_name} created'

    def run_query_file(self, file_path):
        with open(file_path) as f:
            query_text = f.read()
            self._create_conn()
            res = self.conn.execute(query_text)
            self._close_conn()
        return res
