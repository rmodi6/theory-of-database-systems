with ERZIPCODES as (
    select substr(ZIPCODE, 1, 5) as ERZIPCODE
    from CSE532.FACILITY F
             inner join CSE532.FACILITYCERTIFICATION FC
                        on F.FACILITYID = FC.FACILITYID and ATTRIBUTEVALUE = 'Emergency Department'
),
     ERZIPCODES_WITH_SHAPE as (
         select ERZIPCODE, SHAPE as ERZIPCODE_SHAPE
         from ERZIPCODES
                  inner join CSE532.USZIP on ERZIPCODE = ZCTA5CE10
     ),
     NEIGHBOR_ERZIPCODES as (
         select substr(ZIPCODE, 1, 5) as NEIGHBOR_ERZIPCODE
         from CSE532.FACILITY F
                  inner join CSE532.USZIP on substr(ZIPCODE, 1, 5) = ZCTA5CE10
                  inner join ERZIPCODES_WITH_SHAPE on DB2GSE.ST_Intersects(SHAPE, ERZIPCODE_SHAPE) = 1
     )
select distinct substr(ZIPCODE, 1, 5) as NOERZIPCODE
from CSE532.FACILITY
where substr(ZIPCODE, 1, 5) not in
      (select ERZIPCODE from ERZIPCODES union select NEIGHBOR_ERZIPCODE from NEIGHBOR_ERZIPCODES);