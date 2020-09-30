import os
import logging
import pandas as pd
import sqlalchemy as sa
from enum import Enum
from sqlalchemy import create_engine

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
# project_dir = os.path.join(os.environ['HOME'], 'PycharmProjects', 'datacore')
project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'datacore')
modules_dir = os.path.join(project_dir, 'modules')
database_dir = os.path.join(project_dir, 'data')

env = os.environ
