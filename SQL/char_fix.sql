-- Test environment
SELECT *
FROM wp_liftenergypitt.wp_usermeta
where meta_value like '%\t%';

update wp_liftenergypitt.wp_usermeta
set meta_value = left(meta_value, length(meta_value) - 1)
where meta_value like '%\t%';
select umeta_id
     , user_id
     , meta_key
     , left(meta_value, length(meta_value) - 1) as meta_fix
from wp_liftenergypitt.wp_usermeta
where meta_value like '%\t%';