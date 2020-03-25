INSERT INTO CSE532.FACILITY (facilityid, facilityname, description, address1, address2, city, state, zipcode,
                             countycode, county, geolocation)
    (SELECT facilityid,
            facilityname,
            description,
            address1,
            address2,
            city,
            state,
            zipcode,
            countycode,
            county,
            DB2GSE.ST_POINT(LONGITUDE, LATITUDE, 1)
     FROM CSE532.FACILITYORIGINAL);
