select column_name, concat(TABLE_SCHEMA, '.', TABLE_NAME) as location
from information_schema.columns
where TABLE_SCHEMA = 'wp_postmeta_pivot'
and table_name not like '\_%'