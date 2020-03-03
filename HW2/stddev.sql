-- Create custom data type to store salaries in an array --
CREATE OR REPLACE TYPE DECARRAY AS DECIMAL(9,2) ARRAY[INTEGER]@

-- Create stored procedure --
CREATE OR REPLACE PROCEDURE salary_std_dev(OUT SALARYSTDDEV DOUBLE)
    LANGUAGE SQL
    BEGIN

        -- Create variables --
        DECLARE i INTEGER;
        DECLARE n DOUBLE;
        DECLARE avg_salary DOUBLE;
        DECLARE salary_array DECARRAY;

        -- Fetch all salaries into salary_array --
        SET i = 1;
        FOR v1 AS c1 CURSOR FOR
            SELECT salary FROM EMPLOYEE
        DO
            SET salary_array[i] = salary;
            SET i = i + 1;
        END FOR;

        -- Compute average salary --
        SET avg_salary = 0;
        SET n = CARDINALITY(salary_array);
        SET i = 1;
        WHILE i <= n DO
            SET avg_salary = avg_salary + salary_array[i];
            SET i = i + 1;
        END WHILE;
        SET avg_salary = avg_salary / n;

        -- Compute standard deviation using formula sqrt(sum((X - mu)^2) / n) --
        SET SALARYSTDDEV = 0;
        SET i = 1;
        WHILE i <= n DO
            SET SALARYSTDDEV = SALARYSTDDEV + POWER((salary_array[i] - avg_salary), 2);
            SET i = i + 1;
        END WHILE;
        SET SALARYSTDDEV = POWER(SALARYSTDDEV / n, 0.5);

    END@

-- Call the stored procedure --
CALL salary_std_dev(?)@