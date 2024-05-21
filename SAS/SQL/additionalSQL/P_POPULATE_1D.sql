--------------------------------------------------------
--  DDL for Procedure P_POPULATE_1D
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POPULATE_1D" 
(
        in_dimension_type       VARCHAR2 ,
        in_table_nm             VARCHAR2
) AS

/* ----------------------------------------------------------------------------

Change History:

$Log: 5150_MDI_p_populate_1D.sql,v $
Revision 1.1.2.2  2009/02/12 17:04:17  makirk
Fix for S0562011

Revision 1.1.2.1  2008/12/02 15:54:09  dirapa
No comment given.
Added for rename from 5100_MDI_p_populate_1D.sql.
See originally named file for history prior to the rename.

Revision 1.1.2.1  2008/11/26 18:43:56  dirapa
No comment given.

Revision 1.1  2008/05/16 19:38:37  dirapa
-- MMMR61226


---------------------------------------------------------------------------- */

n_sqlnum          NUMBER(10)      := 1000;
t_proc_name       VARCHAR2(30)    := 'p_populate_1D';
t_call            VARCHAR2(1000);
v_sql             VARCHAR2(4000)  := NULL;
t_error_level     VARCHAR2(6)     := 'info';
t_error_msg       VARCHAR2(1000);
t_sql2            VARCHAR2(255)   := NULL;
t_sql3            VARCHAR2(255)   := NULL;


t_lowest_level    NUMBER(10,0);
t_dim_table_nm    VARCHAR2(100);
t_column_nm       VARCHAR2(100);
t_current_year    NUMBER(10,0);
t_history_year    NUMBER(10,0) ;
t_feature_year    NUMBER(10,0) ;
t_cnt             NUMBER(10,0);
t_period          VARCHAR2(100);
t_lv1loc_id       NUMBER(10,0);

BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name                               || ' ( '||
        COALESCE(in_dimension_type, 'NULL') || ',' ||
        COALESCE(in_table_nm, 'NULL')       || ',' ||
        ' ) ';

maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);
--COMMIT;

n_sqlnum := 2000;

IF  UPPER(in_dimension_type) NOT IN  ('L', 'M', 'T' ) THEN
        BEGIN
                RAISE_APPLICATION_ERROR(-20001,'InValid Dimension type.');
        END;
END IF;

n_sqlnum := 3000;

BEGIN
        v_sql := 'SELECT COALESCE(1,0) FROM datamgr.fact_table WHERE table_name = ' || '''' || UPPER(in_table_nm) || '''';
        EXECUTE IMMEDIATE v_sql INTO t_cnt;
EXCEPTION
        WHEN NO_DATA_FOUND THEN
                t_cnt := 0;
END;

IF  t_cnt != 1   THEN
        BEGIN
                RAISE_APPLICATION_ERROR(-20001,'Invalid Table Name');
        END;
END IF;

n_sqlnum := 4000;
BEGIN
        v_sql := 'DELETE FROM maxdata.' ||  UPPER(in_table_nm) ;
EXECUTE IMMEDIATE v_sql;
EXCEPTION
        WHEN OTHERS THEN
        NULL;
END;

n_sqlnum := 5000;

If UPPER(in_dimension_type) = 'L' THEN

        n_sqlnum := 5100;

        BEGIN
                v_sql := 'SELECT lowest_loc_level FROM datamgr.fact_table WHERE table_name = ' ||  '''' || UPPER(in_table_nm) || '''';
                EXECUTE IMMEDIATE v_sql INTO t_lowest_level;
        END;

        n_sqlnum := 5200;

        BEGIN
                FOR i IN 1..t_lowest_level
                LOOP
                        IF i = 1 THEN
                                SELECT lv1loc_id INTO t_lv1loc_id FROM maxdata.lv1loc WHERE num_user1 = 1;
                        END IF;

                        t_dim_table_nm := 'lv' || i || 'loc';
                        t_column_nm    := 'lv' || i || 'loc_id';

                        n_sqlnum := 5300;

                        v_sql := 'INSERT INTO maxdata.' || UPPER(in_table_nm) || ' (location_level,location_id) SELECT ' || i || ',' || t_column_nm || ' FROM maxdata.' || t_dim_table_nm || ' WHERE lv1loc_id = ' || t_lv1loc_id ;
                        EXECUTE IMMEDIATE v_sql;
                END LOOP;
        END;
ELSIF  UPPER(in_dimension_type) ='M' THEN

        n_sqlnum := 6000;

        BEGIN
                v_sql := 'SELECT lowest_merch_level FROM  datamgr.fact_table where table_name = ' ||  '''' || UPPER(in_table_nm) || '''';
                EXECUTE IMMEDIATE v_sql INTO t_lowest_level;
        END;

        n_sqlnum := 6100;

        BEGIN
                FOR i IN 1..t_lowest_level
                LOOP
                        IF i =1 then
                                t_dim_table_nm := 'lv1cmast';
                                t_column_nm    := 'lv1cmast_id';
                        ELSIF i = 10 THEN
                                t_dim_table_nm := 'lv10mast';
                                t_column_nm    := 'lv10mast_id';
                        ELSE
                                t_dim_table_nm := 'lv'||i||'ctree';
                                t_column_nm    := 'lv'||i||'ctree_id';
                        END IF;

                        n_sqlnum := 6200;
                        v_sql :='INSERT INTO maxdata.'|| UPPER(in_table_nm) || ' (merch_level,merch_id) SELECT ' || i || ','|| t_column_nm || ' FROM maxdata.'|| t_dim_table_nm || ' WHERE record_type = ' || '''' || 'L' || '''' ;
                        EXECUTE IMMEDIATE v_sql;
                END LOOP;
        END;
ELSIF UPPER(in_dimension_type) ='T' THEN

        n_sqlnum := 7000;

        BEGIN
                v_sql := 'SELECT lowest_time_level, COALESCE(history_year_no,0), COALESCE(feature_year_no,0) FROM  datamgr.fact_table WHERE table_name = ' ||  '''' || UPPER(in_table_nm) || '''';
                EXECUTE IMMEDIATE v_sql INTO t_lowest_level, t_history_year, t_feature_year;
        END;

        n_sqlnum := 7100;
        t_current_year  := TO_NUMBER(TO_CHAR(sysdate,'yyyy'));
        t_history_year := t_current_year - t_history_year;
        t_feature_year := t_current_year + t_feature_year;

        FOR j IN t_history_year..t_feature_year
        LOOP
                FOR i IN 47..t_lowest_level
                LOOP
                        t_cnt := 0;

                        IF i = 47 THEN
                                BEGIN
                                        SELECT 1 INTO t_cnt FROM maxdata.path_seg WHERE higherlevel_id = 47;
                                EXCEPTION
                                        WHEN OTHERS THEN
                                                t_cnt := 0;
                                END;
                        ELSE
                                BEGIN
                                        SELECT 1 INTO t_cnt FROM maxdata.path_seg WHERE lowerlevel_id = i;
                                EXCEPTION
                                        WHEN OTHERS THEN
                                             t_cnt := 0;
                                END;
                        END IF;


                        IF t_cnt = 1 THEN
                                t_dim_table_nm := 'lv'||((i+1)-47)||'time';
                                t_column_nm := 'lv'||((i+1)-47)||'time_lkup_id';
                                t_period    := 'lv'||((i+1)-47)||'time_id';

                                n_sqlnum := 7300;
                                v_sql :='INSERT INTO maxdata.'|| UPPER(in_table_nm) || '(time_level,time_id,period,cycle) SELECT ' || i || ','|| t_column_nm|| ',' || t_period || ' , cycle_id FROM  maxapp.'||t_dim_table_nm|| ' WHERE cycle_id = ' || j;
                                --dbms_output.put_line(v_sql);
                                EXECUTE IMMEDIATE v_sql;

                        END IF;
                END LOOP;
        END LOOP;
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

                t_sql2 := substr(v_sql,1,255);
                t_sql3 := substr(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_POPULATE_1D" TO "MADMAX";
