CREATE OR REPLACE PROCEDURE salary_std_dev(OUT SALARYSTDDEV DOUBLE)
    LANGUAGE SQL
    BEGIN

        SET SALARYSTDDEV = (select power((sum(power(SALARY, 2)) / count(SALARY)) -
                                         (power(sum(cast(SALARY as double)) / count(SALARY), 2)), 0.5) from EMPLOYEE);

    END@

CALL salary_std_dev(?)@