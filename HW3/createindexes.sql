drop index cse532.facilityidx;
drop index cse532.zipidx;
drop index cse532.facilityididx;
drop index cse532.attributevalueidx;
drop index cse532.zipcodeidx;
drop index cse532.zcta5ce10idx;

create index cse532.facilityidx on cse532.facility (geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip (shape) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.facilityididx on cse532.FACILITYCERTIFICATION (FACILITYID);

create index cse532.attributevalueidx on cse532.FACILITYCERTIFICATION (ATTRIBUTEVALUE);

create index cse532.zipcodeidx on cse532.FACILITY (ZIPCODE);

create index cse532.zcta5ce10idx on cse532.USZIP (ZCTA5CE10);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;
