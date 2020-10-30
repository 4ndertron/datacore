# insert into wp_liftenergypitt.wp_postmeta ( meta_key
#                                           , meta_value)
# todo: more work needs to be done. This may need to be split into two parts.
#   1: append new values, as a post_id, into the postmeta table.
#   2: replace existing values, with a post_id, in the postmeta table.
select meta_key
     , field_id
from (
         select m.meta_key
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
           and m.post_id = 1262
     ) as pmcv