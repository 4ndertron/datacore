import os
import re
import json
import pickle
import logging
import platform
import numpy as np
import unicodedata
import pandas as pd
import datetime as dt
import requests as rq
import sqlalchemy as sa
from .module_enums import Regex
from .module_enums import SQLText
from .module_enums import Messages
from .module_enums import DateTimes
from .module_enums import HandlerParams
from .module_enums import JobNimbusToWPEngineMapping
from .module_enums import DfColumnConversion
from modules.Handlers.data_handler import DataHandler
from modules.Handlers.rest_api_handler import ApiHandler
from modules.Handlers.gs_handler import GSheetHandler

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
operating_system = platform.system()
if operating_system == 'Windows':
    home_dir = os.environ['userprofile']
else:
    home_dir = os.environ['HOME']

user_dl_dir = os.path.join(home_dir, 'Downloads')
project_dir = os.getcwd()  # Returns the directory of the __main__ file.
modules_dir = os.path.join(project_dir, 'modules')
database_dir = os.path.join(project_dir, 'data')
secrets_dir = os.path.join(project_dir, 'secrets')
temp_dir = os.path.join(project_dir, 'temp')
sql_dir = os.path.join(project_dir, 'SQL')
dt_format = DateTimes.date_string_format_text.value

env = os.environ
JSON = 'json'
XML = 'xml'
