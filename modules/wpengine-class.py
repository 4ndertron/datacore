from . import os
import mysql.connector  # pip install mysql-connector-python


class WPEngineDb:
    """
    pittsql is a class made to connect to a mysql server
    hosted on the WP Engine.

    SETUP
    set environment variables to securely store login information.
    Get the login information for your instance by following the
    instructions found at:
    https://wpengine.com/support/setting-remote-database-access/
    """

    def __init__(self, **kwargs):
        self._host = kwargs['host'] if 'host' in kwargs else os.environ['thepitt-db-host']
        self._port = kwargs['port'] if 'port' in kwargs else os.environ['thepitt-db-port']
        self._user = kwargs['user'] if 'user' in kwargs else os.environ['thepitt-db-user']
        self._pswd = kwargs['pswd'] if 'pswd' in kwargs else os.environ['thepitt-db-pswd']
        self.conn = None
        self.cur = None


pitt_db = mysql.connector.connect(
    host='liftenergypitt.sftp.wpengine.com',
    user='liftenergypitt',
    password='QUyTPoGFUq3E5BI1hzwd',
    port=13306
)

print(pitt_db)
