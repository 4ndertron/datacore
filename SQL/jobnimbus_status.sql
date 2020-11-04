-- Job search attempt
select distinct c.status, c.Stage
from jobnimbus.contact as c
where c.financing like '%sunnova%'
  and c.stage != 'Sold'
;

-- Record type status list (only ists all if there are jobs in every status).
select distinct j.Status, j.`Record Type`
from jobnimbus.job as j
where j.`Record Type` in ('Sunnova', 'Solkraft', 'SunRun', 'Sunlight', 'LoanPal', 'Cash')
order by 2 desc, 1
;

-- Count of unique status' for each job record type (financier).
select j.`Record Type`, count(j.Status) as ct
from (
         select distinct ji.`Record Type`, ji.Status
         from jobnimbus.job as ji
         where ji.`Record Type` in ('Sunnova', 'Solkraft', 'SunRun', 'Sunlight', 'LoanPal', 'Cash')
     ) as j
group by j.`Record Type`
;

-- Unique contact record types.
select distinct c.`Record Type`
from jobnimbus.contact as c
;

-- Jobs with no site survey work-order
-- Potential source of "active" accounts that need to be moved over to The Pitt.
-- todo: fix the contact table to include the Id column
select j.`Record Type`
     , j.Id
     , j.`Contact Id`
     , w.`Contact Id`
     , j.Status
     , w.`Record Type`
     , j.`Sales Rep`
from jobnimbus.job as j
         left join (select * from jobnimbus.work_order where `Record Type` = 'Site Survey') as w
                   on w.`Contact Id` = j.`Contact Id`
where w.`Record Type` is null
  and j.`Record Type` in ('Sunnova', 'Solkraft', 'SunRun', 'Sunlight', 'LoanPal', 'Cash')
and j.Status != 'Cancellation Confirmed'
;