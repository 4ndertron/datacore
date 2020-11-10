select n.post_id    as system_post_id
     , a.meta_value as account_post_id
     , n.meta_value as customer_name
     , l.meta_value as location
from ( -- customer name sub query
         select *
         from wp_liftenergypitt.wp_postmeta
         where meta_key = 'tp_homeowner_name'
     ) as n
         left join
     ( -- name-location bridge sub query
         select *
         from wp_liftenergypitt.wp_postmeta
         where meta_key = 'tp_account'
     ) as a
     on a.post_id = n.post_id
         left join
     ( -- location sub query
         select *
         from wp_liftenergypitt.wp_postmeta as m
         where m.meta_key = 'tp_location'
     ) as l
     on l.post_id = a.meta_value
;

select jnc.Id
     , jnc.Display
     , concat(`Address Line`,
              concat(', ', City),
              concat(', ', State),
#               concat(', ', Zip),
              concat(', USA')) as account_id
from jobnimbus.contact as jnc
where coalesce(if(`Address Line` = '', True, null),
               if(City = '', True, null),
#                if(Zip = '', True, null),
               if(State = '', True, null)) is null
;

select jn.Id
     , jn.account_id
     , tp.location
     , tp.account_post_id
from (select jnc.Id
           , jnc.Display
           , concat(`Address Line`,
                    concat(', ', City),
                    concat(', ', State),
                    concat(', ', Zip),
                    concat(', USA')) as account_id
      from jobnimbus.contact as jnc
      where coalesce(if(`Address Line` = '', True, null),
                     if(City = '', True, null),
                     if(Zip = '', True, null),
                     if(State = '', True, null)) is null) as jn
         right join
     (select n.post_id    as system_post_id
           , a.meta_value as account_post_id
           , n.meta_value as customer_name
           , l.meta_value as location
      from (select *
            from wp_liftenergypitt.wp_postmeta
            where meta_key = 'tp_homeowner_name') as n
               left join
           (select *
            from wp_liftenergypitt.wp_postmeta
            where meta_key = 'tp_account') as a
           on a.post_id = n.post_id
               left join
           (select *
            from wp_liftenergypitt.wp_postmeta as m
            where m.meta_key = 'tp_location') as l
           on l.post_id = a.meta_value) as tp
     on tp.location = jn.account_id
;
