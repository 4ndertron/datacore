import os
import pandas as pd
import sqlalchemy as sa
from enum import Enum
from sqlalchemy import create_engine


project_dir = os.path.join(os.environ['HOME'], 'PycharmProjects', 'datacore')
modules_dir = os.path.join(project_dir, 'modules')
database_dir = os.path.join(project_dir, 'data')
data_type_conversions = {'NoneType': 'NULL',
                         'int': 'INTEGER',
                         'float': 'REAL',
                         'str': 'TEXT',
                         # todo: add date type conversion
                         'bytes': 'BLOB'}
