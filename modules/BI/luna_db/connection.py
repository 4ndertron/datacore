from . import *
import psycopg2


class PostgresConnectionHandler:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.database = 'postgres'
        self.username = None
        self.password = None
        self.host = None,
        self.port = '5432'

        self.active_connection = False
        self.network_timeout = 600

        self.is_closed = True

        self.ready_to_connect = False

        self.console_output = False

    def console_messages_on(self):
        self.console_output = True

    def console_messages_off(self):
        self.console_output = False

    def set_network_timeout(self, seconds):
        self.network_timeout = int(seconds)
        if self.active_connection:
            self.close_connection()
            self.open_connection()

    def login(self, host, username=None, password=None):
        self.host = host
        if username and password:
            self.username = username
            self.password = password
        else:
            self.username = os.environ.get('POSTGRES_USER')
            self.password = os.environ.get('POSTGRES_PASS')
        self.ready_to_connect = True

    def check_connection(self):
        if self.active_connection:
            self.is_closed = bool(self.connection.closed)

    def open_connection(self):
        try:
            self.connection = psycopg2.connect(database=self.database,
                                               user=self.username,
                                               password=self.password,
                                               host=self.host,
                                               port='5432',
                                               connect_timeout=self.network_timeout,
                                               )
            self.active_connection = True
            self.check_connection()
            if self.console_output:
                print('Opened Connection')
        except Exception as e:
            raise e

    def close_connection(self):
        try:
            if not self.is_closed:
                self.connection.close()
        except:
            pass
        self.check_connection()
        if self.console_output:
            print('Closed Connection')

    def _open_cursor(self):
        self.check_connection()
        if self.is_closed:
            self.open_connection()
        self.cursor = self.connection.cursor()
        if self.console_output:
            print('Opened Cursor')

    def _close_cursor(self):
        if self.cursor:
            self.cursor.close()
            if self.console_output:
                print('Closed Cursor')
