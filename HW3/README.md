# Homework 3

## Query 2
### Execution times
Table below lists the execution times in ms for query 2 with and without indexes:

|Without Index (ms)|With Index (ms)|Time Difference (ms)|
|:----------------:|:------------:|:-------------------:|
|       109        |      36      |         73          |

## Query 3

### Assumptions
Please note the following assumptions while finding zipcodes with no ER facilities:
- It is assumed that `ZCTA5CE10` column in the `CSE532.USZIP` table is the column containing data for the corresponding 
zipcode. 
- Some entries in the `ZIPCODE` column of `CSE532.FACILITY` table have 9 digit zipcodes of the format 
`xxxxx-xxxx`. To successfully join them with the USZIP table, `subtr` function is used to consider only the first 5 
digits. Consequently, the result table has only unique 5 digit zipcodes from the FACILITY table.
- There are 23 zipcodes in the FACILITY table that do not have an entry in the USZIP table and have no ER
Department. Since it is not possible to find neighbors of these zipcodes, these are also included in the final output.

### Execution times
Table below lists the execution times for query 3 with and without indexes:

|Without Index (s)|With Index (s)|Time Difference (ms)|
|:---------------:|:------------:|:------------------:|
|      1.82       |     1.76     |         60         |

