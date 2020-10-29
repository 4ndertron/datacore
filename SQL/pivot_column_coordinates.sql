select column_name, concat(TABLE_SCHEMA, '.', TABLE_NAME) as location
from information_schema.columns
where TABLE_SCHEMA = 'wp_postmeta_pivot'
  and table_name not like '\_%';

select distinct m.meta_key
              , concat(TABLE_SCHEMA, '.', TABLE_NAME) as pivot_location
              , p.post_type
from wp_liftenergypitt.wp_posts as p
         left join wp_liftenergypitt.wp_postmeta as m
                   on p.ID = m.post_id
         left join information_schema.columns as c
                   on m.meta_key = c.COLUMN_NAME
where m.meta_key not like '\_%'
  and concat(TABLE_SCHEMA, '.', TABLE_NAME) is not null;