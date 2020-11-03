select distinct col.TABLE_SCHEMA, col.table_name, col.column_name, m.meta_key, m.meta_value
from information_schema.columns as col
         left join wp_liftenergypitt.wp_postmeta as m
                   on m.meta_key = col.column_name
where column_name != 'post_id'
  and col.TABLE_SCHEMA like '%postmeta%'
  and col.table_name like '\_%'
  and m.meta_value like '%field%'