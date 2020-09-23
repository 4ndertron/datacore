from enum import Enum
from . import sa


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


class SQLText(Enum):
    test_pivot_original = sa.text("""
    select * 
    from wp_liftenergypitt.wp_postmeta 
    where meta_key not like '\_%'
    """)
    post_types_and_columns = sa.text('''
    select distinct pm.meta_key
                  , p.post_type 
    from wp_liftenergypitt.wp_postmeta as pm 
        left join wp_liftenergypitt.wp_posts as p 
            on p.ID = pm.post_id
    ''')
