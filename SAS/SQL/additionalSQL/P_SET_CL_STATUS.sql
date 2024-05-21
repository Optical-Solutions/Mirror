--------------------------------------------------------
--  DDL for Procedure P_SET_CL_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SET_CL_STATUS" (
        in_change_code  CHAR,
        in_id           NUMBER,
        in_level        NUMBER,
        in_future1      NUMBER,
        in_future2      NUMBER,
        in_future3      NUMBER
) AS
/*----------------------------------------------------------------------
$Log: 2162_p_set_cl_status.sql,v $
Revision 1.10  2007/06/19 14:39:35  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.6  2006/03/27 17:22:13  makirk
Wrapped commit in a change_code check (ref defect id: S0350496)

Revision 1.5  2006/02/17 22:18:55  healja
Replace $id with $Log
 2162_p_set_cl_status.sql,v 1.4 2005/09/16 19:18:07 joscho Exp $

Change History

V6.1
6.1.0-001 07/19/05 Diwakar      Re Written for 6.1

Usage: Both External (used by the app) and Internal.

Description:

This procedure is to be used to mark clsuter history status to 'OB' (obsolete)
for the passed in change code with the given ID and/or Level.

Below Change Codes are supported:

__________________________________________________________________________________________________________________________________________
|Change                 |in_change_code |in_id          |in_level       |Who calls the procedure        |Procedure action
__________________________________________________________________________________________________________________________________________
| New Mfinc Load        | 'N'           |Load time ID   | Load time     |Data manager calls this        |All entries whose start/end
|                       |               |OR -1          | Level         |procedure                      |date covers the loading
|                       |               |               |               |                               |period are marked as OB.
|                       |               |               |               |                               |If passed-in ID is
|                       |               |               |               |                               |1, then make all entries OB.
|                       |               |               |               |                               |It is the case that multiple
|                       |               |               |               |                               |periods of other fact tables
|                       |               |               |               |                               |(MCOMP, etc) are affected.
__________________________________________________________________________________________________________________________________________
|                       |               |               |               |                               |
|Back loading of any    | 'B'           |  -1           |  -1           |DBA who does back loading      |All entries marked as 'OB'
|fact tables            |               |               |               |                               |
__________________________________________________________________________________________________________________________________________
|                       |               |               |               |                               |
|Reclassification for   | 'C'           |  -1           |  -1           |DBA who does reclassification  |All entries marked as 'OB'
|Merch and Loc          |               |               |               |                               |
__________________________________________________________________________________________________________________________________________
|                       |               |               |               |                               |
|Loading fact tables    | 'F'           |  -1           |  -1           |DBA who loads fact tables      |All entries marked as 'OB'
|other than MFINC       |               |               |               |                               |
__________________________________________________________________________________________________________________________________________

|Cluster Definition     | 'D'           | ID of the     |  -1           |The application (cluster tool) |All entries that use the
|Change                 |               |changed clsuter|               |calls this procedure when a    |changed cluster are marked 'OB'
|                       |               |               |               |cluster definition is changed. |
__________________________________________________________________________________________________________________________________________

|Cluster Set Definition | 'S'           | ID of the     |  -1           |The application (cluster tool) |All entries that use the
|Change                 |               |changed clsuter|               |calls this procedure when a    |changed cluster are marked 'OB'
|                       |               |set            |               |cluster set definition         |
|                       |               |               |               |is changed.                    |
__________________________________________________________________________________________________________________________________________

|Change in Worksheet    | 'W' or 'P'    |Worksheet ID   |  -1           |Application calls whenever     |All entries related to the given
|boundary               |               |               |               |worksheet boundaries were      |worksheet marked as 'OB'
|                       |'P' for PA     |               |               |changed                        |
|                       |worksheet      |               |               |                               |
|                       |               |               |               |                               |Known Issue: When time boundary changed,
|                       |               |               |               |                               |entries should be updated for the
|                       |               |               |               |                               |new time bodundary.
__________________________________________________________________________________________________________________________________________
|Multifact_column Table |               |               |               |                               |
|data changed           | 'M'           |  -1           |  -1           |Trigger on multifact_column    |
|                       |               |               |               |calls this procedure.          |
__________________________________________________________________________________________________________________________________________


Parameters:

in_change_code  : Type of activity to be done.
in_id           : Time_id if its Mfinc Load or Cluster ID if its Cluster Definition change or Worksheet ID if its Worksheet boundry change.
                  In all other changes -1 is passed.
in_level        : If time Id for Mfinc load is given, time level also should be given. For all other changes -1 will be passed in.
in_future1      : placeholder. Pass in -1.
in_future2      : placeholder. Pass in -1.
in_future3      : placeholder. Pass in -1.
--------------------------------------------------------------------------------*/

n_sqlnum                        NUMBER(10,0);
t_proc_name                     VARCHAR2(32)     := 'p_set_cl_status';
t_error_level                   VARCHAR2(6)      := 'info';
t_call                          VARCHAR2(1000);
v_sql                           VARCHAR2(1000)   := NULL;
t_sql2                          VARCHAR2(255);
t_sql3                          VARCHAR2(255);

t_start_date                    DATE;
t_end_date                      DATE;
tbl_level                       NUMBER(10);
t_cnt                           NUMBER(10);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure
t_call := t_proc_name || ' ( ' ||
        COALESCE(in_change_code, 'NULL')  || ',' ||
        COALESCE(in_id, -1) || ',' ||
        COALESCE(in_level, -1) || ',' ||
        COALESCE(in_future1, -1) || ',' ||
        COALESCE(in_future2, -1) || ',' ||
        COALESCE(in_future3, -1) ||
        ' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

n_sqlnum := 2000;

-- Check inputs

IF in_change_code NOT IN ('N','B','C','F','D','M', 'W') THEN
BEGIN
   RAISE_APPLICATION_ERROR (-20001,'Unsupported activity_code: '||in_change_code);
END;
END IF;

IF in_change_code IN  ('D', 'W', 'S') AND (in_id IS NULL OR in_id = 0) THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001, 'ID required for the specified activity_code ' ||in_change_code);
END;
END IF;

IF in_change_code IN  ('N') AND
        ((in_id IS NULL OR in_id = 0) OR
         (in_level IS NULL OR in_level = 0)) THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001,'The Passed in value for ID or Level cannot be null or Zero when change code = ''N''');
END;
END IF;

-- End of checking inputs

IF in_change_code IN ('B','C','F','M') THEN  -- Back load, reclassification, fact tables other than mfinc, multifact_column change
BEGIN
        UPDATE maxdata.cl_hist_status SET status = 'OB';
END;
ELSIF in_change_code = 'N' THEN -- New data load
BEGIN
        -- in_level/in_id are the level/id of mfinc/mcomp/etc load.
        -- If id=-1, then many periods were affected by the load.
        -- Mark all entries as obsolete.

        IF in_id = -1 THEN
        BEGIN
                UPDATE maxdata.cl_hist_status SET status = 'OB';
        END;
        ELSE
        BEGIN
                tbl_level := in_level - 46;

                v_sql := 'SELECT ' ||
                         'lv' || tbl_level ||'time_start_date, ' ||
                         'lv' || tbl_level ||'time_end_date ' ||
                         'FROM maxapp.lv' || tbl_level || 'time '       ||
                         'WHERE lv' || tbl_level ||'time_lkup_id = :from_time_id';

                EXECUTE IMMEDIATE  v_sql INTO t_start_date,t_end_date USING in_id;

                -- Mark as obsolete all the entries which cover the start/end dates of
                -- the loading period.

                UPDATE maxdata.cl_hist_status
                SET status = 'OB'
                WHERE dv_start_date <= t_start_date
                AND dv_end_date >= t_end_date;
        END;
        END IF; -- if id=-1 else
END;
ELSIF in_change_code IN  ('W','P') THEN -- Worksheet change
BEGIN
        UPDATE maxdata.cl_hist_status
        SET     status = 'OB'
        WHERE planworksheet_id = in_id;
END;
ELSIF in_change_code = 'S' THEN -- cluster Set change
BEGIN
        -- Invalidate the entries which use the change cluster set.

        UPDATE maxdata.cl_hist_status c
        SET status = 'OB'
        WHERE EXISTS
                (SELECT *
                FROM maxdata.planworksheet p
                WHERE p.planworksheet_id = c.planworksheet_id
                AND loc_path_id > 1000 -- cluster
                AND from_loc_level < 1000 -- cluster set
                AND from_loc_level <> 4
                AND loc_path_id - 1000 = in_id); -- cluster set id
END;
ELSIF in_change_code = 'D' THEN -- Definition change of a cluster
BEGIN
        v_sql := 'TRUNCATE TABLE maxdata.t_cl_id';

        EXECUTE IMMEDIATE v_sql;

        INSERT INTO maxdata.t_cl_id (pw_gs_id, clst_id)
                SELECT c.planworksheet_id, p.from_loc_id
                FROM maxdata.cl_hist_status c, maxdata.planworksheet p
                WHERE c.planworksheet_id = p.planworksheet_id
                AND p.from_loc_level > 1000
                AND p.from_loc_id = in_id
                AND c.status = 'OK'
                UNION ALL
                SELECT p.planworksheet_id, s.fnl_clstr_spc_id
                FROM maxdata.cl_hist_status c, maxdata.planworksheet p, maxdata.clstr_str s
                WHERE c.planworksheet_id = p.planworksheet_id
                AND p.from_loc_level < 1000
                AND s.clstr_st_id = (p.loc_path_id - 1000)
                AND s.fnl_clstr_spc_id = in_id
                AND c.status = 'OK';

                UPDATE maxdata.cl_hist_status c
                SET status = 'OB'
                WHERE c.planworksheet_id IN (SELECT DISTINCT pw_gs_id FROM maxdata.t_cl_id t);
END;
END IF;

-- Change_multifact_column, with code 'M'.
-- Check to see if it "M" type so it doesn't commit in a trigger
IF in_change_code != 'M' THEN
        COMMIT;
END IF;


EXCEPTION
        WHEN OTHERS THEN
                -- Check to see if it "M" type so it doesn't rollback in a trigger
                IF in_change_code != 'M' THEN
                        ROLLBACK;
                END IF;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := substr(v_sql,1,255);
                        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';

                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);

                -- Check to see if it "M" type so it doesn't commit in a trigger
                IF in_change_code != 'M' THEN
                        COMMIT;
                END IF;

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_SET_CL_STATUS" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."P_SET_CL_STATUS" TO "MADMAX";
