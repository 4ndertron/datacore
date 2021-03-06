select distinct m.meta_key
              , concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) as pivot_location
              , p.post_type
#               , v.meta_value                          as field_id
from wp_liftenergypitt.wp_posts as p
         left join wp_liftenergypitt.wp_postmeta as m
                   on p.ID = m.post_id
         left join information_schema.columns as c
                   on m.meta_key = c.COLUMN_NAME
#          left join (
#     select pm.post_id
#          , pm.meta_key
#          , pm.meta_value
#     from wp_liftenergypitt.wp_postmeta as pm
#     where pm.meta_key like '\_%'
# ) as v
#                    on v.meta_key = concat('_', m.meta_key)
where m.meta_key not like '\_%'d
  and concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) is not null
;


select distinct m.meta_key
              , concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) as pivot_location
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
where concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) is not null;