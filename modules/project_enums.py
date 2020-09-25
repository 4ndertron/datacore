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
    distinct_post_types = sa.text('''
    select distinct p.post_type
    from wp_liftenergypitt.wp_posts as p
    ''')
    post_type_meta_collection = sa.text(f'''
    select pm.post_id
         , pm.meta_key
         , pm.meta_value
    from wp_liftenergypitt.wp_postmeta as pm
        left join wp_liftenergypitt.wp_posts as p
            on p.ID = pm.post_id
    where meta_key %s
    and p.post_type = '%s'
    ''')
    post_types_and_columns = sa.text('''
    select distinct pm.meta_key
                  , p.post_type 
    from wp_liftenergypitt.wp_postmeta as pm 
        left join wp_liftenergypitt.wp_posts as p 
            on p.ID = pm.post_id
    ''')
    union_part = sa.text('''
    select mpv.post_id as post_id
    , '%s' as meta_key
    , mpv.%s as meta_value
    from %s as mpv
    ''')
