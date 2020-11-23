from . import *
from enum import Enum


class HandlerParams(Enum):
    dialect = 'dialect'
    driver = 'driver'
    user = 'user'
    pswd = 'pswd'
    host = 'host'
    port = 'port'
    database = 'data'
    conn_args = 'conn_args'
    valid_params = [dialect,
                    driver,
                    user,
                    pswd,
                    host,
                    port,
                    database,
                    conn_args]


class WpEntities(Enum):
    comment = 'comment'
    post = 'post'
    term = 'term'
    user = 'user'


class JobNimbus_to_WPEngine_Mapping(Enum):
    """
    All of the dictionary types represent the WP Engine structure.
    All of the list types represent the JobNimbus column names.
    """
    post_template = {
        # Default keys that need to be populated:
        #   non-calculated fields:
        #       post_author
        #       post_type
        #   auto-calculated fields:
        #       post_title
        #           - counter in the db
        #       post_name
        #           - same as post_title
        #       guid
        #           - site address + post_title
        #       post_date
        #           - execution timestamp
        #       post_date_gmt
        #           - execution timestamp
        #       post_modified
        #           - execution timestamp
        #       post_modified_gmt
        'post_author': [],
        'post_date': [],
        'post_date_gmt': [],
        'post_content': [''],
        'post_title': [],  # This column needs to have its value calculated beforehand
        'post_excerpt': [''],
        'post_status': ['publish'],
        'comment_status': ['closed'],
        'ping_status': ['closed'],
        'post_password': [''],
        'post_name': [],  # This column needs to have its value calculated beforehand
        'to_ping': [''],
        'pinged': [''],
        'post_modified': [],
        'post_modified_gmt': [],
        'post_content_filtered': [''],
        'post_parent': [0],
        'guid': [],  # This column needs to have its value calculated beforehand
        'menu_order': [0],
        'post_type': [],
        'post_mime_type': [''],
        'comment_count': [0]
    }
    conversion_map = {
        'account': {
            'post': post_template,
            'post_meta': {
                'tp_location': ['Address Line', 'City', 'State', 'Country'],
                # 'tp_location': ['account_id'],
            }
        },
        'system': {
            'post': post_template,
            'post_meta': {
                # 'tp_account': [], # this needs to be populated with the post_id of the account entity.
                'tp_homeowner_name': ['Display'],
                'tp_team_members_sales_rep': ['Sales Rep'],
                'tp_homeowner_phone': ['Main Phone'],
                'tp_site_hoa': ['HOA'],
                'tp_contract_job_type': ['Record Type'],
                'tp_homeowner_contact_method': ['Preferred Method Of Contact'],
                # 'tp_notes_0_message': ['Note'],
                'tp_notes_0_timestamp': ['Date Created'],
                'tp_notes_2_posted_by': ['Created By'],
                'tp_ops_progress_install_waiting_on_mpu_or_other': ['MPU'],
                # 'tp_ops_progress_install_install_complete': ['Installation Complete'],
                'tp_ops_progress_nem_pto_nem_submitted': ['NEM Submitted'],
                'tp_site_roof_type': ['Roof Type'],
                'tp_site_gate_code': ['Gate Code'],
                'tp_site_jurisdiction': ['Jurisdiction'],
                'tp_team_members_designer': ['Designer'],
                'tp_utility_company': ['Utility Co.'],
                'tp_utility_meter_number': ['Meter #'],
                'tp_utility_nem_aggregate': ['NEM Aggregate'],
            },
        },
        'proposal': {
            'post': post_template,
            'post_meta': {
                'tp_proposal_financier': ['Financing'],
                'tp_proposal_system_size': ['System Size'],
                'tp_proposal_inverter_type': ['Monitoring'],
            },
        },
    }


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
    jn_export_entity_rt = r'All (.*) Columns'


class Regex(Enum):
    re_mod = re
    derive_post_from_table_name = re.compile(RegexText.derive_post_from_table_name_rt.value)
    wpengine_table_prefix = re.compile(RegexText.wpengine_table_prefix_rt.value)
    wpengine_meta_suffix = re.compile(RegexText.wpengine_meta_suffix_rt.value)
    pivot_schema_suffix = re.compile(RegexText.pivot_schema_suffix_rt.value)
    jn_export_entity = re.compile(RegexText.jn_export_entity_rt.value)


class DateTimes(Enum):
    date_string_format_text = '%Y-%m-%d %H:%M:%S'


class PivotTypes(Enum):
    vaild_pivot_strings = ['comments', 'posts', 'terms', 'users']


class SQLText(Enum):
    select_distinct_meta_keys = sa.text('''
    select distinct meta_key
    from wp_liftenergypitt.%s;
    ''')
    select_account_tally = sa.text('''
    select max(cast(post_title as decimal)) as idcount
    from wp_liftenergypitt.wp_posts
    where post_type = '%s'
    ''')
    select_account_post_ids = sa.text('''
    select post_id, meta_value
    from wp_liftenergypitt.wp_postmeta as m
    where m.meta_key = 'tp_location'
    ''')
    select_distinct_jobnimbus_accounts = sa.text('''
    select distinct concat(`Address Line`,
                       concat(', ', City),
                       concat(', ', State),
                       concat(', USA')) as account_id
    from jobnimbus.contact as jnc
    where coalesce(if(`Address Line` = '', True, null),
                   if(City = '', True, null),
                   if(State = '', True, null)) is null
    ''')
    select_pivot_column_metadata = sa.text('''
    select distinct m.meta_key
                  , concat(TABLE_SCHEMA, '.', TABLE_NAME) as pivot_location
                  , p.post_type
                  , v.meta_value                          as field_id
    from wp_liftenergypitt.wp_posts as p
             left join wp_liftenergypitt.wp_postmeta as m
                       on p.ID = m.post_id
             left join information_schema.columns as c
                       on m.meta_key = c.COLUMN_NAME
             left join (
        select pm.post_id
             , pm.meta_key
             , pm.meta_value
        from wp_liftenergypitt.wp_postmeta as pm
        where pm.meta_key like '\_%'
    ) as v
                       on v.meta_key = concat('_', m.meta_key)
    where m.meta_key not like '\_%'
      and concat(TABLE_SCHEMA, '.', TABLE_NAME) is not null
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
    where table_schema = %s;
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
    select_all_from_schema_table = sa.text('''
    select * from %s.%s;
    ''')
