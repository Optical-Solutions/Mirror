--------------------------------------------------------
--  DDL for Procedure P_WKLY_TIME_PERIOD_SP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_TIME_PERIOD_SP" IS 
/*Step 1 */
    p_process_id      NUMBER;
    jobno             BINARY_INTEGER;
    parm_start_week   VARCHAR2(25);
    parm_end_week     VARCHAR2(25);
BEGIN

/* Clear Table to Insert New Weeks*/
    EXECUTE IMMEDIATE 'Truncate Table wkly_sas_prod_time_pd';
    UPDATE sas_process_sw
        SET
            process_time = SYSDATE,
            process_complete = 'false';

    COMMIT;
    
    INSERT INTO sas_process_log_id (
        process_ind,
        process_date,
        process_type
    ) VALUES (
        'I',
        SYSDATE,
        'WEEKLY'
    );

    COMMIT;

/* Get Process_id from Log */
    SELECT
        MAX(process_id)
    INTO
        p_process_id
    FROM
        sas_process_log_id
    WHERE
        process_type = 'WEEKLY'; 
/*Process/Insert Week*/

--    INSERT INTO wkly_sas_prod_time_pd SELECT
--        merchandising_year,
--        merchandising_week
--    FROM
--        (
--            SELECT
--                *
--            FROM
--                sas_process_calendar
--            WHERE
--                processed = 'N'
--            ORDER BY
--                1,
--                2
--        )
--    WHERE
--        ROWNUM < 5;


    IF
        SQL%rowcount = 0
    THEN
        return;
    END IF;

  Insert Into Wkly_Sas_Prod_Time_Pd values ('2022','1');
  Insert Into Wkly_Sas_Prod_Time_Pd values ('2022','2');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','13');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','14');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','15');

    COMMIT;

/*Write to Log*/
    INSERT INTO sas_process_log (
        process_id,
        process_step,
        process_name,
        process_start_time,
        process_table,
        process_ind
    ) VALUES (
        p_process_id,
        10,
        'Step 1:Get weeks to process:p_wkly_time_period SPECIAL',
        SYSDATE,
        'Wkly_Sas_Prod_Time_Pd',
        'I'
    );

    COMMIT;

/*Get Ending week */
    SELECT
        merch_year
         || '.'
         || lpad(
            merch_week,
            2,
            '0'
        )
    INTO
        parm_end_week
    FROM
        (
            SELECT
                *
            FROM
                wkly_sas_prod_time_pd
            ORDER BY
                merch_year DESC,
                merch_week DESC
        )
    WHERE
        ROWNUM = 1;

/*Get Starting Week*/

    SELECT
        merch_year
         || '.'
         || lpad(
            merch_week,
            2,
            '0'
        )
    INTO
        parm_start_week
    FROM
        (
            SELECT
                *
            FROM
                wkly_sas_prod_time_pd
            ORDER BY
                merch_year ASC,
                merch_week ASC
        )
    WHERE
        ROWNUM = 1;

    UPDATE sas_process_log
        SET
            process_ind = 'C',
            process_end_time = SYSDATE,
            process_name = 'Step 1:Get weeks to process:p_wkly_time_period: ' || parm_start_week || ' thru ' || parm_end_week
    WHERE
            process_step = 10
        AND
            process_id = p_process_id;

    COMMIT;
    
    UPDATE sas_process_log
        SET
            process_ind = 'C',
            process_end_time = SYSDATE
    WHERE
            process_step = 10
        AND
            process_id = p_process_id;

    COMMIT;
    
    dbms_job.submit(
        jobno,
        'P_Wkly_Inventory_Extract();',
        SYSDATE,
        NULL
    );
    COMMIT;
END p_wkly_time_period_sp;

/
