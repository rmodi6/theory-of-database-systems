with zip_mme as (
    select a.BUYER_ZIP                                     as zip,
           SUM(a.MME) / b.ZPOP                             as mme_per_person,
           RANK() over (order by SUM(a.MME) / b.ZPOP desc) as rank
    from CSE532.DEA_NY a
             inner join ZIPPOP b on a.BUYER_ZIP = b.ZIP and b.ZPOP > 0
    group by a.BUYER_ZIP, b.ZPOP
)
select *
from zip_mme
where rank < 6;