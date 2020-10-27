#  Common imports stored at top level
import snowflake.connector

from . import *


# %% Snowflake Connection Handler V2
class SnowflakeConnectionHandlerV2:
    def __init__(self):
        self.sf = snowflake.connector

        self.connection = None
        self.schema = 'VSLR'
        self.role = 'D_POST_INSTALL_SUPER_SEC_R'
        self.active_connection = False

        self.user = None
        self.account = None
        self.username = None
        self.password = None

        self.network_timeout = 1200

        self._is_closed = True

        self.ready_to_connect = False

        self.console_output = False

    def _set_user(self, user):
        self.user = user
        self.get_credentials()

    def _set_schema(self, schema):
        if schema != self.schema:
            self.schema = schema
            if self.active_connection:
                self._close_connection()
                self._open_connection()

    def is_closed(self):
        return self._is_closed

    def set_role(self, role):
        if role != self.role:
            self.role = role
            if self.active_connection:
                self._close_connection()
                self._open_connection()

    def set_network_timeout(self, seconds):
        self.network_timeout = int(seconds)

    def console_messages_on(self):
        self.console_output = True

    def console_messages_off(self):
        self.console_output = False

    def get_credentials(self):
        credentials = json.loads(os.environ.get('SNOWFLAKE_KEY')).get(self.user.upper())
        self.username = credentials.get('USERNAME')
        self.password = credentials.get('PASSWORD')
        self.account = credentials.get('ACCOUNT')
        self.ready_to_connect = True

    def _verify_connection(self):
        if self.connection:
            if self.connection.is_closed():
                self._open_connection()
        else:
            self._open_connection()

    def _open_connection(self):
        self.connection = self.sf.connect(
            user=self.username,
            password=self.password,
            account=self.account,
            schema=self.schema,
            role=self.role,
            network_timeout=self.network_timeout,
            client_session_keep_alive=True
        )
        self.active_connection = True
        if self.console_output:
            print('Opened Connection')

    def _close_connection(self):
        self.active_connection = False
        self.connection.close()
        if self.console_output:
            print('Closed Connection')

    def get_cursor(self):
        self._verify_connection()
        if self.console_output:
            print('Opened Cursor')
        return self.connection.cursor()
