import re
from enum import Enum
from . import sa


class HandlerParams(Enum):
    dialect = 'dialect'
    driver = 'driver'
    user = 'user'
    pswd = 'pswd'
    host = 'host'
    port = 'port'
    database = 'database'
    conn_args = 'conn_args'
    valid_params = [dialect,
                    driver,
                    user,
                    pswd,
                    host,
                    port,
                    database,
                    conn_args]


class Engines(Enum):
    local_wp_engine_name = 'local_wp_engine'
    pitt_engine_name = 'pitt_engine'
    sqlite_engine_name = 'sqlite_engine'


class Messages(Enum):
    error = 'You found an unintentional feature!'
    no_valid_parameters = 'No valid parameters found. No changes were made.'
    updated_valid_parameters = 'Updated the following valid parameters: '
    no_tables = 'Sorry, no operations could be completed on an empty list of tables.'
    no_schemas = 'Sorry, no operations could be completed on an empty list of schemas.'
    invalid_table_type = '%s is not a table that this program is prepared to handle.'
    invalid_schema_type = '%s is not a schema that this program is prepared to handle.'


class RegexText(Enum):
    derive_post_from_table_name_rt = r'^[^_+]*'
    wpengine_table_prefix_rt = r'wp_'
    wpengine_meta_suffix_rt = r'meta'
    pivot_schema_suffix_rt = r'_pivot'


class Regex(Enum):
    re_mod = re
    derive_post_from_table_name = re.compile(RegexText.derive_post_from_table_name_rt.value)
    wpengine_table_prefix = re.compile(RegexText.wpengine_table_prefix_rt.value)
    wpengine_meta_suffix = re.compile(RegexText.wpengine_meta_suffix_rt.value)
    pivot_schema_suffix = re.compile(RegexText.pivot_schema_suffix_rt.value)


class DateTimes(Enum):
    date_string_format_text = '%Y-%m-%d %H:%M:%S'


class PivotTypes(Enum):
    vaild_pivot_strings = ['comments', 'posts', 'terms', 'users']


class SQLText(Enum):
    select_distinct_meta_keys = sa.text('''
    select distinct meta_key
    from wp_liftenergypitt.%s;
    ''')
    post_type_meta_collection_split = sa.text(f'''
    select pm.post_id
         , pm.meta_key
         , pm.meta_value
    from wp_liftenergypitt.wp_postmeta as pm
        left join wp_liftenergypitt.wp_posts as p
            on p.ID = pm.post_id
    where meta_key %s
    and p.post_type = '%s';
    ''')
    post_type_meta_collection_join = sa.text(f'''
    select pm.post_id
         , pm.meta_key
         , pm.meta_value
    from wp_liftenergypitt.wp_postmeta as pm
        left join wp_liftenergypitt.wp_posts as p
            on p.ID = pm.post_id
    where p.post_type = '%s';
    ''')
    post_types_and_columns = sa.text('''
    select distinct pm.meta_key
                  , p.post_type 
    from wp_liftenergypitt.wp_postmeta as pm 
        left join wp_liftenergypitt.wp_posts as p 
            on p.ID = pm.post_id;
    ''')
    union_part = sa.text('''
    select mpv.post_id as post_id
    , '%s' as meta_key
    , mpv.%s as meta_value
    from %s as mpv;
    ''')
    select_schema_tables = sa.text('''
    select table_name
    from information_schema.tables
    where table_schema = '%s';
    ''')
    backup_table_ddl = sa.text('''
    create table db_table_backups
    (
        "index" BIGINT,
        event_datetime TEXT,
        backup_source_engine TEXT,
        backup_destination_engine TEXT,
        backup_table TEXT,
        table_count BIGINT,
        backup_table_name text not null
    );
    
    create index ix_db_table_backups_index
        on db_table_backups ("index");
    ''')
    create_schema_sql = sa.text('''
    create schema %s;
    ''')
    select_schemas = sa.text('''
    select *
    from information_schema.schemata;
    ''')
    select_backup_tables = sa.text('''
    select * from db_table_backups;
    ''')
    select_all_from_table = sa.text('''
    select * from wp_liftenergypitt.%s;
    ''')
