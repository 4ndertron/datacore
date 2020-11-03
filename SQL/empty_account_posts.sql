select p.ID, m.meta_value
from wp_liftenergypitt.wp_postmeta as m
         right join wp_liftenergypitt.wp_posts as p
                    on p.ID = m.post_id
where p.post_type = 'account'
  and m.meta_value is null