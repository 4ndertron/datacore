import os
import re

project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'datacore')
modules_dir = os.path.join(project_dir, 'modules')
database_dir = os.path.join(project_dir, 'data')
data_type_conversions = {'NoneType': 'NULL',
                         'int': 'INTEGER',
                         'float': 'REAL',
                         'str': 'TEXT',
                         # todo: add date type conversion
                         'bytes': 'BLOB'}
