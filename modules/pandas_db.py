from . import pd
from . import os
from . import create_engine


if __name__ == '__main__':
    env = os.environ
    pitt_host = env['thepitt_db_host']
    pitt_port = env['thepitt_db_port']
    pitt_user = env['thepitt_db_user']
    pitt_pswd = env['thepitt_db_pswd']

    pitt_url = f'mysql://{pitt_user}:{pitt_pswd}@{pitt_host}:{pitt_port}'
    sqlite_engine = create_engine('sqlite:///../data/foo.db')
    mysql_engine = create_engine(pitt_url)
