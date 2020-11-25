with windows as (
    select cast(wt.window_id as unsigned)   as window_id
         , wt.status_start
         , wt.status_end
         , cast(wt.start_index as unsigned) as start_index
         , cast(wt.end_index as unsigned)   as end_index
         , wt.goal
         , wt.actual
         , wt.window_name
    from jobnimbus.status_windows as wt
)

   , boards as (
    select bt.record
         , bt.bucket
         , bt.status
         , cast(bt.status_index as unsigned) as status_index
         , cast(bt.bucket_index as unsigned) as bucket_index
         , cast(bt.record_index as unsigned) as record_index
    from jobnimbus.board as bt
    where bt.record = 'Contact'
)

   , contacts as (
    select *
    from jobnimbus.contact as ct
)

   , status_changes as (
    select *
    from jobnimbus.status_changes as sct
)

   , contact_status_change as (
    select c.Id
         , coalesce(s.customer_name, c.Display)                    as customer_name
         , lag(s.status) over (
        partition by coalesce(s.customer_name, c.Display)
        order by coalesce(s.date_status_changed, c.`Date Status Change`)
        )                                                          as old_status
         , coalesce(s.status, c.Status)                            as new_status
         , coalesce(s.date_status_changed, c.`Date Status Change`) as date_status_changed
         , ifnull(lead(s.date_status_changed) over (
        partition by coalesce(s.customer_name, c.Display)
        order by coalesce(s.date_status_changed, c.`Date Status Change`)
        ), current_timestamp)                                      as lead_change_date
    from status_changes as s
             left join contacts as c
                       on s.customer_name = c.Display
)

   , contact_status_change_gap as (
    select csc.Id
         , customer_name
         , old_status
         , new_status
         , date_status_changed
         , lead_change_date
         , timediff(lead_change_date, date_status_changed)                      as hour_change_gap
         , time_to_sec(timediff(lead_change_date, date_status_changed)) / 86400 as day_change_gap
    from contact_status_change as csc
)

   , window_board as (
    select *
    from windows as w
             left join boards as b
                       on b.record_index between w.start_index and w.end_index
)

   , contact_status_window as (
    select *
    from window_board as wb
             left join contact_status_change_gap as cscg
                       on cscg.new_status = wb.status
)

   , main as (
    select csw.window_name
         , csw.status
         , csw.start_index
         , csw.end_index
         , count(case
                     when csw.new_status != csw.status_end
                         then csw.Id end)           as ct
         , avg(case
                   when csw.new_status != csw.status_end
                       then csw.day_change_gap end) as avg
    from contact_status_window as csw
    group by csw.window_name, csw.status, csw.start_index, csw.end_index
)

   , view_cte as (
    select *
#     from windows
#     from boards
#     from contacts
#     from status_changes
#     from contact_status_change_gap
#     from status_change_metric
#     from window_board
#     from examine_window_board
    from contact_status_window
)

select *
from main
;
