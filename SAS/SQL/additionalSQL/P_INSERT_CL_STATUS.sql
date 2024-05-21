--------------------------------------------------------
--  DDL for Procedure P_INSERT_CL_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_INSERT_CL_STATUS" (
    in_pw_id        NUMBER,
    in_kpi_dv_id    NUMBER,
    in_src_pw_id    NUMBER, -- See Parameter description below.
    in_future2      NUMBER,
    in_future3      NUMBER
) AS

/* ----------------------------------------------------------------------
Change History

$Log: 2154_p_insert_cl_status.sql,v $
Revision 1.9.8.1  2008/11/03 22:54:37  makirk
Fixed minor flaw in how parameters are logged

Revision 1.9  2007/06/19 14:39:37  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5  2006/01/09 19:22:58  makirk
Modified for creating temp tables under maxtemp



V6.1
6.1.0-001 06/09/05 Diwakar  Re-written for 6.1

Description:
This procedures creates / inserts records into maxdata.cl_hist_status table.
Cluster History table name is generated in this procedure and any other routines
has to get the cluster history table name from the cl_hist_status table for the
corresponding worksheet_id and kpi_dv_id.

Parameters:
in_pw_id:     Planworksheet ID for the create / insert into cl_hist_status table.
in_kpi_dv_id: Dataversion ID for the inserted / create worksheet ID
in_src_pw_id: If  -1, then insert a new entry.
              Else copy the given source worksheet's entries to the target worksheet (in_pw_id)
              Used only by the db procedure. The app always passes in -1.
in_future2:   Placeholder. Pass in -1.
in_future3:   Placeholder. Pass in -1.
-------------------------------------------------------------------------------- */

n_sqlnum        NUMBER(10,0);
t_proc_name     VARCHAR2(32)    := 'p_insert_cl_status';
t_error_level   VARCHAR2(6)     := 'info';
t_call          VARCHAR2(4000);
v_sql           VARCHAR2(4000)  := NULL;
t_sql2          VARCHAR2(255);
t_sql3          VARCHAR2(255);

v_sql2          VARCHAR2(4000)  := NULL;
t_table_nm      VARCHAR2(64);
t_col_value     VARCHAR2(100);

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name           || ' ('||
    COALESCE (in_pw_id, -1)     || ',' ||       -- NVL(int, 'NULL') returns error because of diff datatype.
    COALESCE (in_kpi_dv_id, -1) || ',' ||
    COALESCE (in_src_pw_id, -1) || ',' ||
    COALESCE (in_future2, -1)   || ',' ||
    COALESCE (in_future3, -1)   || ')';

maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;

n_sqlnum := 2000;
IF in_src_pw_id IS NULL THEN
    RAISE_APPLICATION_ERROR(-20001,'Parameter source PW ID can not be null');
END IF;

n_sqlnum := 4000;

t_table_nm := 'MAXTEMP.T_CL' || '_' ||  in_pw_id || '_' || in_kpi_dv_id;

n_sqlnum := 5000;

IF in_src_pw_id = -1 THEN
    BEGIN
    INSERT INTO maxdata.cl_hist_status
        (
            planworksheet_id,
            kpi_dv_id,
            table_nm
        )
    VALUES
        (
            in_pw_id,
            in_kpi_dv_id,
            t_table_nm
        );
    END;
ELSE
    BEGIN
    DECLARE CURSOR c_cl_hist_cols IS
    SELECT UPPER(column_name) column_name FROM user_tab_columns
    WHERE table_name = 'CL_HIST_STATUS'
    ORDER BY column_name;
    BEGIN

    v_sql := NULL;
    v_sql2 := NULL;

    FOR c1 IN c_cl_hist_cols LOOP
        n_sqlnum := 5100;
        BEGIN
        IF v_sql IS NULL  THEN
            v_sql :='INSERT INTO maxdata.cl_hist_status ( ';
        ELSE
            v_sql := v_sql ||',';
        END IF;
        END;


        v_sql := v_sql || c1.column_name;

        n_sqlnum := 5200;

        --WARNING: The column names are fetched in alphabetical order, so the columns
        --in the EXECUTE IMMEDIATE USING clause has to be alphabetical order also.

        t_col_value :=  CASE c1.column_name
                    WHEN 'KPI_DV_ID' THEN ':in_kpi_dv_id'
                    WHEN 'PLANWORKSHEET_ID' THEN ':in_pw_id'
                    WHEN 'TABLE_NM' THEN ':t_table_nm'
                    ELSE c1.column_name
                END;

        n_sqlnum := 5300;
        BEGIN
        IF v_sql2 IS NULL  THEN
            v_sql2 := ' ) SELECT ';
        ELSE
            v_sql2 := v_sql2 ||',';
        END IF;
        END;

        v_sql2 := v_sql2 || t_col_value;

    END LOOP;
    END;

    n_sqlnum := 5400;

    EXECUTE IMMEDIATE v_sql || v_sql2 ||
            ' FROM maxdata.cl_hist_status ' ||
            ' WHERE planworksheet_id = :in_src_pw_id ' ||
            '   AND kpi_dv_id = :in_kpi_dv_id '
    USING   in_kpi_dv_id,in_pw_id, t_table_nm, in_src_pw_id, in_kpi_dv_id;
    END; -- declare cursor
END IF;

COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

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

        t_sql2 := SUBSTR(v_sql,1,255);
        t_sql3 := SUBSTR(v_sql,256,255);
        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
        COMMIT;

        RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_INSERT_CL_STATUS" TO "MADMAX";
