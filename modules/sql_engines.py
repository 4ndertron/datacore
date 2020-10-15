from sqlalchemy import create_engine
from modules.project_enums import Engines
from modules.project_enums import HandlerParams as hp
from modules.project_enums import Messages


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
        self._dbapi = '+mysqlconnector' if 'wpengine' in self._host else ''
        self.valid_parameters = hp.valid_params.value
        self.engine = None
        self._setup_engine()

    def _setup_engine(self):
        conn_args = {
            'auth_plugin': 'mysql_native_password',
        }
        url = f'mysql{self._dbapi}://{self._user}:{self._pswd}@{self._host}:{self._port}'
        self.engine = create_engine(url, connect_args=conn_args)

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


class EngineHandler:
    def __init__(self, *args, **kwargs):
        self.valid_parameters = hp.valid_params.value
        self.dialect = kwargs['dialect'] if 'dialect' in kwargs else ''
        self.driver = kwargs['driver'] if 'driver' in kwargs else ''
        self.user = kwargs['user'] if 'user' in kwargs else ''
        self.pswd = kwargs['pswd'] if 'pswd' in kwargs else ''
        self.host = kwargs['host'] if 'host' in kwargs else ''
        self.port = kwargs['port'] if 'port' in kwargs else ''
        self.database = kwargs['database'] if 'database' in kwargs else ''
        self.conn_args = kwargs['conn_args'] if 'conn_args' in kwargs else ''
        self.engine = None
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

    def update_connection_parameters(self, **kwargs):
        self.dialect = kwargs['dialect'] if 'dialect' in kwargs else self.dialect
        self.driver = kwargs['driver'] if 'driver' in kwargs else self.driver
        self.user = kwargs['user'] if 'user' in kwargs else self.user
        self.pswd = kwargs['pswd'] if 'pswd' in kwargs else self.pswd
        self.host = kwargs['host'] if 'host' in kwargs else self.host
        self.port = kwargs['port'] if 'port' in kwargs else self.port
        self.database = kwargs['database'] if 'database' in kwargs else self.database
        self.conn_args = kwargs['conn_args'] if 'conn_args' in kwargs else self.conn_args
        returns = []
        for k, v in kwargs:
            if k in self.valid_parameters:
                returns.append(k)
        if len(returns) == 0:
            return Messages.no_valid_parameters.value
        else:
            self._setup_engine()
            return f'{Messages.updated_valid_parameters.value}{",".join(returns)}'
