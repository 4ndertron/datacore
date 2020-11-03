select t.meta_value,
       t.ct,
       m.post_id,
       p.post_author,
       u.display_name,
       p.post_date,
       p.ID,
       p.post_name
from (
         select m.meta_value, count(m.meta_value) as ct
         from wp_liftenergypitt.wp_postmeta as m
         where m.meta_key = 'tp_location'
         group by m.meta_value
         order by 2 desc
     ) as t
         left join wp_liftenergypitt.wp_postmeta as m
                   on m.meta_value = t.meta_value
         left join wp_liftenergypitt.wp_posts as p
                   on p.ID = m.post_id
         left join wp_liftenergypitt.wp_users as u
                   on u.ID = p.post_author
where t.ct > 1
order by 1 asc