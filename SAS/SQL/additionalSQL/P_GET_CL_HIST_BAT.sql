--------------------------------------------------------
--  DDL for Procedure P_GET_CL_HIST_BAT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_CL_HIST_BAT" (
        in_duration_hours       NUMBER  -- Duration of this batch job in number of hours
) AS

/*
------------------------------------------------------------------------------
Change History:
$Log: 2179_p_get_cl_hist_bat.sql,v $
Revision 1.9  2007/06/19 14:39:29  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5  2006/05/05 12:58:16  joscho
Make cluster batch and cube cleanup mutually exclusive.

Revision 1.4  2006/04/14 16:39:29  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

Revision 1.3  2005/11/23 13:42:29  joscho
Update it for 6.1.
1. p_get_cl_hist interface change
2. p_drop_cl_hist_dv
3. Remove gridsets -- under 6.1, PA is now in PLANWORKSHEET, not GRIDSETS.


-- Change History:
-- V6.1
-- 6.1.0-004 07/29/05   Joseph  6.1 code cleanup
-- V5.6
-- 5.6.0-029-12 05/18/04Sachin  #16473 Delete old entries from cl_hist_status table.
-- 5.6.0-029 03/18/04   Diwakar Added Errorhandler to list several Ids
-- 5.6.0-016 12/18/03   Diwakar Moved commit out from the Begin block during the cleanup of cl_hist table.
-- V5.4
-- 5.4.0-028 10/30/02   Sachin  Added exception handlers for delete stmts
-- 5.4.0-018 10/2/02    Helmi   add clean up code for CL_status and errhndling to the loop
-- 5.4.0-000 8/26/02    Sachin  Added code to handle timeshift, dv_id = 21
-- 5.4.0-000 8/20/02    DR      Changed 1 to t_pw_gs_id for Dynamic Id parameter when calling p_get_cl_hist_53 procedure.
---V5.3.4
-- 07/31/03     Sachin          Support for 'ER'
-- 2/12/03      Sachin          Updated Error handler
-- V5.3.2
-- 05/14/02     Rg              Use n_time_tmpl_id instead of n_event_time_id
-- V5.3.1
-- 03/14/2002   Joseph Cho      Get num_of_periods from template.

-- V5.3
-- 03/01/2002   Joseph Cho      Get num_of_periods using from_time_id.
-- 03/01/2002   Joseph Cho      Suppress 'drop cl hist' error.
-- 02/26/2002   Joseph Cho      Change params to support seeding bridge.
-- 2/20/2002    Joseph cho      Replace p_get_cl_hist with p_get_cl_hist_53.

-- V5.2.4.3
-- 1/22/2002    Joseph cho      Initial entry.

-- Description:
-- This procedure loops through  cluster history status entries and
-- pre-aggregate cluster histories as necessary.
----------------------------------------------------------------------------------
*/

n_sqlnum           NUMBER          := 0;
v_sql              VARCHAR2(1000);
n_valid_days       NUMBER;
d_expiration_date  DATE;
d_start_batch_time DATE;
d_end_batch_time   DATE;
n_shutdown         NUMBER;
v_status           VARCHAR2(2);
n_cnt              NUMBER;
t_row_cnt          NUMBER;
t_error_found      NUMBER          := 0;
t_failed_ids       VARCHAR2(50)    := ' ';
t_failed_cnt       NUMBER;
v_proc_name        VARCHAR2(20)    := 'p_get_cl_hist_bat';
t_future           NUMBER          := -1;
t_debug_flg        NUMBER          := 0;
t_table_nm         VARCHAR2(1024);

BEGIN
--dbms_output.put_line(in_duration_hours);

v_sql := 'Start of Batch Processing';
maxdata.ins_import_log (v_proc_name,'info', v_sql, NULL, NULL, NULL);

COMMIT;


-- If T_CUBE tables are being cleaned up, then return.

n_sqlnum := 400;

SELECT COUNT(*) INTO t_row_cnt
FROM maxdata.WLOOw_object_operation
WHERE cube_id = -1000
AND worksheet_template_id = -1000;

IF t_row_cnt <> 0 THEN
    RAISE_APPLICATION_ERROR (-20001, 'Temporary tables are being cleaned up. Wait a few seconds and try again');
END IF;



-- Delete the log records that belong to p_get_cl_hist_53/p_fill_6keys.
-- They leave 'error' logs, so here, we clean
-- 'error' log.
BEGIN
DELETE FROM maxdata.import_log
WHERE log_id IN ('p_get_cl_hist_bat','p_get_cl_hist')
AND log_level= 'error';

COMMIT;

EXCEPTION WHEN OTHERS THEN NULL;
END;


-- Reset shutdown flag.
UPDATE maxapp.userpref
SET value_1 = 0
WHERE key_1 = 'CL_HIST_SHUTDOWN_BATCH_JOB';

COMMIT;

-- Get the current date to find out if any status was expired.
n_sqlnum := 500;

SELECT SYSDATE INTO d_start_batch_time FROM dual;

SELECT value_1
INTO n_valid_days
FROM maxapp.userpref
WHERE key_1 = 'CL_HIST_VALID_DAYS';

d_expiration_date := SYSDATE - n_valid_days;

-- Reset all entries that were failed in a prior run.
n_sqlnum := 800;

UPDATE maxdata.cl_hist_status
SET status = 'NB'
WHERE status = 'ER';

COMMIT;

n_cnt := 0;
t_failed_cnt := 0;

BEGIN -- cleaning the CL_status table from unrelated records
DELETE FROM maxdata.cl_hist_status c
WHERE NOT EXISTS (SELECT * FROM maxdata.planworksheet p
                  WHERE c.planworksheet_id = p.planworksheet_id)
AND c.planworksheet_id IS NOT NULL;

EXCEPTION WHEN OTHERS THEN NULL;
END;

BEGIN
DELETE FROM maxdata.cl_hist_status c
WHERE EXISTS (SELECT * FROM maxdata.planworksheet p
              WHERE c.planworksheet_id = p.planworksheet_id
              AND p.loc_path_id < 1000)
AND c.planworksheet_id IS NOT NULL;

EXCEPTION WHEN OTHERS THEN NULL;
END;

COMMIT;

DECLARE CURSOR status_cur IS
        SELECT * FROM maxdata.cl_hist_status
        WHERE status NOT IN ('IP', 'OK', 'ER')
        OR last_accessed < d_expiration_date
        ORDER BY plan_count DESC;
BEGIN
FOR c1 IN status_cur LOOP
BEGIN
        n_cnt := n_cnt + 1;

        n_sqlnum := 1000 + n_cnt;

        -- If the batch job is executing longer than the duration time, then exit.

        n_sqlnum := 1100 + n_cnt;
        d_end_batch_time := d_start_batch_time + (in_duration_hours / 24);
        --dbms_output.put_line(sysdate||','||d_end_batch_time);

        EXIT WHEN sysdate > d_END_batch_time;

        -- Check if shutdown was requested.

        n_sqlnum := 1200 + n_cnt;

        SELECT value_1 INTO n_shutdown
        FROM maxapp.userpref
        WHERE key_1 = 'CL_HIST_SHUTDOWN_BATCH_JOB';

        EXIT WHEN n_shutdown = 1;


        IF c1.last_accessed < d_expiration_date THEN

                n_sqlnum := 1500 + n_cnt;

                -- Drop the table older than expiration period.
                -- Ignore error if table to be dropped doesn't exist.

                BEGIN
                maxdata.p_drop_cl_hist_dv (
                        c1.planworksheet_id,
                        c1.kpi_dv_id,
                        t_future,       -- placeholder
                        t_future,       -- placeholder
                        t_future);      -- placeholder

                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                COMMIT;
        ELSE
                -- Get the entry for update.
                -- On Oracle, if other process is holding the entry, then it waits
                -- until commit/rollback.

                n_sqlnum := 3000 + n_cnt;

                SELECT status INTO v_status
                FROM maxdata.cl_hist_status
                WHERE planworksheet_id = c1.planworksheet_id
                AND kpi_dv_id = c1.kpi_dv_id
                FOR UPDATE;

                IF v_status <> 'OK' AND v_status <> 'IP' AND v_status <> 'ER' AND c1.cube_id IS NOT NULL THEN
                BEGIN
                        maxdata.p_get_cl_hist (
                                c1.cube_id,             --T_CUBE table key
                                c1.planworksheet_id,    -- planworksheet_id
                                c1.kpi_dv_id,           --
                                t_future,               -- placeholder
                                t_future,               -- placeholder
                                t_debug_flg,
                                t_table_nm);            -- output table name

                EXCEPTION
                        WHEN OTHERS THEN
                                -- If the error was raised within p_get_cl_hist, then
                                -- continue looping
                                -- else there was an error calling the proc (e.g., priv error)
                                --      terminate looping.

                                t_error_found :=1;

                                t_failed_cnt := t_failed_cnt + 1;

                                IF t_failed_cnt <=3 THEN
                                        t_failed_ids := t_failed_ids || c1.planworksheet_id ||',';
                                ELSE
                                        IF t_failed_cnt = 4 THEN
                                                t_failed_ids := t_failed_ids || '...';
                                        END IF;
                                END IF;

                                -- On UDB, we raise exception if calling p_get_cl_hist fails.
                                -- That can happen if the supplied parameters
                                -- don't match with the procedure definition.
                END;

		COMMIT; -- release the UPDATE lock (see above)

                END IF;
        END IF;
        COMMIT;
        EXCEPTION WHEN OTHERS THEN NULL;
END;
END LOOP;

END;

v_sql := 'End of Batch Processing';
maxdata.ins_import_log (v_proc_name,'info', v_sql, NULL, NULL, NULL);

COMMIT;

IF t_error_found = 1 THEN
        v_sql := 'Error in worksheet ' || RTRIM(t_failed_ids) || '. See IMPORT_LOG';
        raise_application_error (-20001,v_sql);
END IF;

EXCEPTION
        WHEN OTHERS THEN
                --rollback;
                COMMIT;

                v_sql := SQLERRM || ' ( ' ||v_proc_name||', SQL#:' || n_sqlnum || ')';

                -- Log the error message.

                maxdata.ins_import_log (v_proc_name,'error', v_sql, NULL, NULL, NULL);

                COMMIT;

                RAISE_APPLICATION_ERROR (-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_GET_CL_HIST_BAT" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_GET_CL_HIST_BAT" TO "MAXUSER";
