--------------------------------------------------------
--  DDL for Procedure P_WL_TRUNCATE_CUBE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_TRUNCATE_CUBE" 
AS
/*------------------------------------------------------------------------------
$Log: 2301_p_wl_truncate_cube.sql,v $
Revision 1.5  2007/06/19 14:40:14  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.1.12.1  2007/06/05 15:32:47  vejang
Moved from 6121 to 612HF4

Revision 1.1.10.3  2007/05/10 18:06:39  anchan
S0423516: Skip checking of connected sessions/jobs

Revision 1.1.10.1  2007/05/09 16:19:07  anchan
S0423516: Remove failed signal row;
          Truncate all t_cube* tables instead of delete;

Revision 1.1  2006/03/02 20:57:53  vejang
From 2364_p_wl_truncate_cube.sql
To    2301_p_wl_truncate_cube.sql
Added for rename from 2364_p_wl_truncate_cube.sql.
See originally named file for history prior to the rename.

Revision 1.6  2006/01/27 21:06:07  anchan
Cosmetic change to the WHILE loop.  Uses [OBJECT OPERATION] table.

Revision 1.5  2006/01/09 14:35:30  anchan
Removed EXEC priv from madmax.

Revision 1.4  2005/12/08 15:15:11  anchan
Added a PAUSE  just prior to TRUNCATE

Revision 1.3  2005/12/07 20:22:18  anchan
GRANT at the end.

Revision 1.1  2005/12/07 20:17:37  anchan
Created

Initial version
makirk
=================================================================================
Description:
Cleans up old data from t_cube_4key, t_cube_loc,t_cube_merch,t_cube_pwid and t_cube_time
tables, while preserving data for active sessions.
NOTE: This procedure should be run during off-hours, as this procedure blocks p_compose_query
for the duration of the execution.
--------------------------------------------------------------------------------*/

BEGIN

DECLARE
    -- Error handling variables
    n_sqlnum        NUMBER(10,0);
    t_proc_name     VARCHAR2(32)   := 'p_wl_truncate_cube';
    t_error_level   VARCHAR2(6)    := 'info';
    t_call          VARCHAR2(1000);
    v_sql           VARCHAR2(1000) := NULL;
    t_sql2          VARCHAR2(255);
    t_sql3          VARCHAR2(255);
    t_str_null      VARCHAR2(255)  := NULL;
    t_int_null      NUMBER(10)     := NULL;
    t_inprogress_flg NUMBER(1);

BEGIN

-- Log the parameters of the procedure
n_sqlnum := 1000;
t_call := t_proc_name || ' ( ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);


--Signal START of the truncate operation:--
n_sqlnum := 2000;
DELETE FROM maxdata.WLOOw_object_operation
WHERE cube_id = -1000
AND worksheet_template_id = -1000;
n_sqlnum := 2100;
INSERT INTO maxdata.WLOOw_object_operation (cube_id,worksheet_template_id,object_type_cd,procedure_nm)
	VALUES (-1000,-1000,-1000,t_proc_name);
COMMIT;


n_sqlnum := 3000;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_4key';

n_sqlnum := 3100;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_4key_cluster';

n_sqlnum := 5000;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_merch';

n_sqlnum := 6000;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_loc';

n_sqlnum := 6100;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_loc_cluster';

n_sqlnum := 7000;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_time';

n_sqlnum := 8000;
EXECUTE IMMEDIATE 'TRUNCATE TABLE maxdata.t_cube_pwid';

--Signal FINISH of the truncate operation:--
n_sqlnum := 9000;
DELETE FROM maxdata.WLOOw_object_operation
WHERE cube_id = -1000
AND worksheet_template_id = -1000;
COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
            ROLLBACK;

            --Remove the signal after a failure:--
            DELETE FROM maxdata.WLOOw_object_operation
            WHERE cube_id = -1000
            AND worksheet_template_id = -1000;
            COMMIT;

            IF v_sql IS NOT NULL THEN
                t_error_level := 'info';
                t_sql2        := 'Most recent dynamic SQL.  Not necessarily related to the current error';
                t_sql3        := SUBSTR(v_sql,1,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
                --COMMIT;
            END IF;

            -- Log the error message
            t_error_level := 'error';
            v_sql := SQLERRM || ' (' || t_call || ', SQL#:' || n_sqlnum || ')';
            t_sql2 := SUBSTR(v_sql,1,255);
            t_sql3 := SUBSTR(v_sql,256,255);
            maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
            --COMMIT;

            RAISE_APPLICATION_ERROR(-20001,v_sql);
END;
END p_wl_truncate_cube;

/
