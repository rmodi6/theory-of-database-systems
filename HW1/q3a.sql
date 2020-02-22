with cte as (
    select CONCAT(YEAR(TRANSACTION_DATE), RIGHT(CONCAT('0', MONTH(TRANSACTION_DATE)), 2)) as monthly_date,
           SUM(DOSAGE_UNIT)                                                               as pill_counts
    from CSE532.DEA_NY
    group by CONCAT(YEAR(TRANSACTION_DATE), RIGHT(CONCAT('0', MONTH(TRANSACTION_DATE)), 2))
)
select monthly_date,
       pill_counts,
       AVG(pill_counts) over (order by monthly_date rows between 1 preceding and 1 following) as smooth_pill_counts
from cte;