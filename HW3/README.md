# Homework 3

## Query 2
### Assumptions
Please note the following assumptions while finding nearest ER facility:
- A buffer zone of **10 miles** is considered to find the nearest ER within this zone.
- `STATUTE MILE` is used as the distance measure.

### Execution times
Table below lists the execution times in ms for query 2 with and without indexes:

|Without Index (ms)|With Index (ms)|Time Difference (ms)|
|:----------------:|:------------:|:-------------------:|
|       109        |      36      |         73          |

## Query 3

### Assumptions
Please note the following assumptions while finding zipcodes with no ER facilities:
- It is assumed that `ZCTA5CE10` column in the `USZIP` table is the column containing data for the corresponding 
zipcode. 
- Some entries in the `ZIPCODE` column of `FACILITY` table have 9 digit zipcodes of the format 
`xxxxx-xxxx`. To successfully join them with the `USZIP` table, `subtr` function is used to consider only the first 5 
digits. Consequently, the result table has only unique 5 digit zipcodes from the `FACILITY` table.
- There are **21** zipcodes in the `FACILITY` table that do not have an ER department and do not have a corresponding 
entry in the `USZIP` table. So it is not possible to find neighbors of these zipcodes, and are **included** in the final 
output.
- There are **2** zipcodes in the `FACILITY` table that do not have a corresponding entry in the `FACILITYCERTIFICATION`
table. The ER facility information about these zipcodes is unknown and hence, these are **not included** in the final 
output.

### Execution times
Table below lists the execution times for query 3 with and without indexes:

|Without Index (s)|With Index (s)|Time Difference (ms)|
|:---------------:|:------------:|:------------------:|
|      1.82       |     1.69     |        130         |

## Query 5
### Assumptions
Please note the following assumptions while merging zipcodes:
- The zipcodes in `ZIPPOP` table do not have preceding 0's. Hence, I have concatenated preceding 0's to make the 
zipcode of length 5.
- Only zipcodes having `population > 0` and having corresponding shape in the `USZIP` table are considered for merging.
- Some zipcodes do not have a neighbor in `USZIP` table and so they cannot be merged but are included in the final 
result.

### Execution
Execute the file `mergezip.sql` as follows:
```bash
db2 -td@ -f mergezip.sql
```
This file creates a temporary table to store the output. It creates a stored procedure `merge_zip(zips_limit)` which has
an input parameter `zips_limit` that can be used to limit the number zipcodes to be considered for merging. By default, 
I have set the parameter to **100** as it takes more than _8_ minutes to execute for all the zipcodes. This parameter 
can be changed where the stored procedure is being called on line which says - `CALL CSE532.merge_zip(100)`.  
In the end, the temporary table is deleted.

### Output format
The output table `MERGED_ZIPCODES` consists of three columns:
- `MERGE_ID`: A unique id. All zipcodes merged into one area will have the same id.
- `ZIPCODE`: Zipcode from the ZIPPOP table which belongs to the given merge_id group.
- `TOTAL_POP`: Sum of population of all zipcodes having the same merge_id i.e. merged together.
