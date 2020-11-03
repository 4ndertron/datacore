select agents.tp_team_members_sales_rep,
       ifnull(count(agents.ID), 0)                                         as proposals,
       ifnull(sum(welcome_calls.system_welcome_calls), 0)                  as welcome_calls,
       ifnull(count(installs.tp_ops_progress_install_install_complete), 0) as installs
from ( -- This is the key table used in the dashboard page. It gathers all systems with an assigned sales rep.
         select p.ID
              , if(m.meta_key = 'tp_team_members_sales_rep', m.meta_value, NULL) as tp_team_members_sales_rep
         from wp_liftenergypitt.wp_posts as p
                  left join wp_liftenergypitt.wp_postmeta as m on p.ID = m.post_id
         where if(m.meta_key = 'tp_team_members_sales_rep', m.meta_value, NULL)
     ) agents
         left join ( -- This table returns the systems that are currently installed
    select p.ID,
           if(m.meta_key = 'tp_ops_progress_install_install_complete', m.meta_value,
              NULL) as tp_ops_progress_install_install_complete
    from wp_liftenergypitt.wp_posts as p
             left join wp_liftenergypitt.wp_postmeta as m
                       on p.ID = m.post_id
    where if(m.meta_key = 'tp_ops_progress_install_install_complete', m.meta_value, NULL)
) as installs
                   on agents.ID = installs.ID
         left join
     ( -- This table will return the cumulative number of welcome calls completed per system
         select wc.ID, count(wc.ID) system_welcome_calls
         from (-- The union all statements are required because welcome calls are queued at the proposal-financer level of an account
-- As financers are added to the site, this query will have to be updated to facilitate those new systems.
-- This is because the query is currently in a static state to reduce server/database overhead.
                  select ID, welcome_call
                  from ( -- Grab Sunnova's welcome calls
                           select p.ID,
                                  if(m.meta_key = 'tp_sunnova_validation_complete', m.meta_value, NULL) as welcome_call
                           from wp_liftenergypitt.wp_posts as p
                                    left join wp_liftenergypitt.wp_postmeta as m
                                              on p.ID = m.post_id
                           where if(m.meta_key = 'tp_sunnova_validation_complete', m.meta_value, NULL)
                       ) as sunnova
                  union all
                  ( -- Grab Sunrun's welcome calls
                      select p.ID, if(m.meta_key = 'tp_sunrun_validation_complete', m.meta_value, NULL) as welcome_call
                      from wp_liftenergypitt.wp_posts as p
                               left join wp_liftenergypitt.wp_postmeta as m
                                         on p.ID = m.post_id
                      where if(m.meta_key = 'tp_sunrun_validation_complete', m.meta_value, NULL)
                  )
                  union all
                  ( -- Grab Sunlight's welcome calls
                      select p.ID, if(m.meta_key = 'tp_sunlight_validation_complete', m.meta_value, NULL) as welcome_call
                      from wp_liftenergypitt.wp_posts as p
                               left join wp_liftenergypitt.wp_postmeta as m
                                         on p.ID = m.post_id
                      where if(m.meta_key = 'tp_sunlight_validation_complete', m.meta_value, NULL)
                  )
                  union all
                  ( -- Grab solkraft's welcome calls
                      select p.ID, if(m.meta_key = 'tp_solkraft_validation_complete', m.meta_value, NULL) as welcome_call
                      from wp_liftenergypitt.wp_posts as p
                               left join wp_liftenergypitt.wp_postmeta as m
                                         on p.ID = m.post_id
                      where if(m.meta_key = 'tp_solkraft_validation_complete', m.meta_value, NULL)
                  )
                  union all
                  ( -- Grab loanpal's welcome calls
                      select p.ID, if(m.meta_key = 'tp_loanpal_validation_complete', m.meta_value, NULL) as welcome_call
                      from wp_liftenergypitt.wp_posts as p
                               left join wp_liftenergypitt.wp_postmeta as m
                                         on p.ID = m.post_id
                      where if(m.meta_key = 'tp_loanpal_validation_complete', m.meta_value, NULL)
                  )
                  union all
                  ( -- Grab cash's welcome calls
                      select p.ID, if(m.meta_key = 'tp_cash_validation_complete', m.meta_value, NULL) as welcome_call
                      from wp_liftenergypitt.wp_posts as p
                               left join wp_liftenergypitt.wp_postmeta as m
                                         on p.ID = m.post_id
                      where if(m.meta_key = 'tp_cash_validation_complete', m.meta_value, NULL)
                  )
              ) as wc
         group by wc.ID
     ) as welcome_calls
     on welcome_calls.ID = agents.ID
group by agents.tp_team_members_sales_rep;