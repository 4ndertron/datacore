select u.display_name, c.display
from jobnimbus.contact as c
         left join wp_liftenergypitt.wp_users as u
                   on u.display_name = c.`Sales Rep`
where u.ID is not null
and u.ID = 40
