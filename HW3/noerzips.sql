with ERZIPCODES as (
    select substr(ZIPCODE, 1, 5) as ERZIPCODE, SHAPE as ERZIPCODE_SHAPE
    from CSE532.FACILITY F
             inner join CSE532.FACILITYCERTIFICATION FC on F.FACILITYID = FC.FACILITYID
             inner join CSE532.USZIP on substr(ZIPCODE, 1, 5) = ZCTA5CE10
    where ATTRIBUTEVALUE = 'Emergency Department'
),
     NEIGHBOR_ERZIPCODES as (
         select substr(ZIPCODE, 1, 5) as NEIGHBOR_ERZIPCODE
         from CSE532.FACILITY F
                  inner join CSE532.USZIP on substr(ZIPCODE, 1, 5) = ZCTA5CE10
                  inner join ERZIPCODES on DB2GSE.ST_Intersects(SHAPE, ERZIPCODE_SHAPE) = 1
     )
select distinct substr(ZIPCODE, 1, 5) as NOERZIPCODE
from CSE532.FACILITY F
where substr(ZIPCODE, 1, 5) not in (select NEIGHBOR_ERZIPCODE from NEIGHBOR_ERZIPCODES);