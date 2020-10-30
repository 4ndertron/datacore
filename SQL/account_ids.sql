select distinct concat(`Address Line`,
                       concat(', ', City),
                       concat(', ', State),
                       concat(', USA')) as account_id
from jobnimbus.contact as jnc
where coalesce(if(`Address Line` = '', True, null),
               if(City = '', True, null),
               if(State = '', True, null)) is null;

select `Address Line`
     , City
     , State
     , 'USA'                   as country
     , concat(`Address Line`,
              concat(', ', City),
              concat(', ', State),
              concat(', USA')) as account_id
from jobnimbus.contact as jnc
where coalesce(if(`Address Line` = '', True, null),
               if(City = '', True, null),
               if(State = '', True, null)) is null
  and `Address Line2` != '';