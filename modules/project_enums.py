from enum import Enum


class HandlerParams(Enum):
    host = 'host'
    port = 'port'
    user = 'user'
    pswd = 'pswd'
    name = 'name'
    valid_params = [host, port, user, pswd, name]


class Engines(Enum):
    local_wp_engine_name = 'local_wp_engine'
    pitt_engine_name = 'pitt_engine'
    sqlite_engine_name = 'sqlite_engine'


class Messages(Enum):
    error = 'You found an unintentional feature!'
    no_valid_parameters = 'No valid parameters found. No changes were made.'
    updated_valid_parameters = 'Updated the following valid parameters: '
