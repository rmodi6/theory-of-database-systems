This file is best viewed in a Markdown reader (eg. https://jbt.github.io/markdown-editor/)

# Homework 2: Standard Deviation in JDBC and Stored Procedure

## JDBC
To run the JDBC code execute the SalaryStdDev.class using the following command:
```bash
java -cp ".:/path/to/db2jcc4.jar" SalaryStdDev databasename tablename login password
```
where `db2jcc4.jar` is the db2 jdbc dependency jar required for executing the code.

## Stored Procedure
Within the command line, create the stored procedure using the following command:
```bash
db2 -td@ -f salary_std_dev_sp.sql
```
This will also call the stored procedure just created and return the output.