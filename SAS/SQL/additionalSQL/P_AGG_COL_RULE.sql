--------------------------------------------------------
--  DDL for Procedure P_AGG_COL_RULE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_COL_RULE" (
    in_table_nm           VARCHAR2,
    in_aggr_hier          VARCHAR2,  -- hierarchy being aggregated:M,L,T,C --C for COUNT.
    in_merch_level      NUMBER,  -- aggr-to level (parent level, not child level)
    in_loc_level          NUMBER,  -- 
    in_time_level          NUMBER,  --
    in_time_id          NUMBER,  --
    in_debug_flg        NUMBER,   -- debug flag, 0=off, other=on
    out_simple_col_list OUT VARCHAR2,   -- output: col list without aggr func
    out_aggr_col_list   OUT VARCHAR2,   -- output: col list with aggr func.
    in_child_first_id   NUMBER:=-1,  --"first" child of the parent time_id; only used for T aggregation
    in_child_last_id    NUMBER:=-1   --"last"  child of the parent time_id; only used for T aggregation
) AS

/* ----------------------------------------------------------------------------
$Log: 5220_IDA_p_agg_col_rule.sql,v $
Revision 1.1.2.1.2.4  2009/06/08 20:38:59  anchan
FIXID S0583005: remove blanks from column names

Revision 1.1.2.1.2.3  2009/05/13 17:49:12  anchan
ENHANCEMENT: allow metadata-driven COUNT columns and aggrules, instead of fixed,harcoded columns.

Revision 1.1.2.1.2.1  2009/03/25 16:15:18  anchan
FIXID S0567466: hardcoded dynamic SQL moved to the agg_rule.db_internal_func column; handle BOP/EOP also

Revision 1.1.2.1  2008/11/26 17:30:08  anchan
FIXID : BASELINE check-in

proc to pick up aggr rules from DATAMGR.COL_AGG_RULE table for In-Database-Aggregation procedures.

Usage: Used by DB only, not by the appl.
Description:
---------------------------------------------------------------------------- */

n_sqlnum            NUMBER(10)    := 1000;
t_proc_name         VARCHAR2(30)    := 'p_agg_col_rule';
t_call              VARCHAR2(1000);
v_sql               VARCHAR2(4000)  := NULL;
t_error_level       VARCHAR2(6)     := 'info';
t_error_msg            VARCHAR2(4000);

-- Remove the variables below if not used
t_count_tbl         VARCHAR2(30) :=in_table_nm||'#COUNT';
t_dim_order         CHAR(2);
t_tbl_id            NUMBER(10)      := -1;
t_time_level        VARCHAR2(1)        :=  NULL; --1,2,3,4,5 for lvxtime
t_rule                 VARCHAR2(100)    := NULL;
t_db_func             VARCHAR2(255)    := NULL;
t_calc                 VARCHAR2(100)    := NULL;
t_rule_col             VARCHAR2(200)    := NULL;
t_col_name          VARCHAR2(30);
t_base_flg          NUMBER(1);
t_debug                NUMBER(10);

BEGIN

-- Log the parameters of the procedure

t_call := t_proc_name                       || ' ( ' ||
    COALESCE(in_table_nm, 'NULL')       || ',' ||
    COALESCE(in_aggr_hier, 'NULL')       || ',' ||
    maxdata.f_num_to_char(in_merch_level)   || ',' ||   
    maxdata.f_num_to_char(in_loc_level)   || ',' ||
    maxdata.f_num_to_char(in_time_level)   || ',' ||
    maxdata.f_num_to_char(in_time_id)   || ',' ||
    maxdata.f_num_to_char(in_debug_flg) || ',' ||
    'OUT,' ||
    'OUT,' ||
    maxdata.f_num_to_char(in_child_first_id) || ',' ||
    maxdata.f_num_to_char(in_child_last_id) || ',' ||
    ' ) ';

maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);
--COMMIT;

--t_debug := 2;
t_debug := in_debug_flg;

-- Check for correct input params.

n_sqlnum := 2000;
IF(in_table_nm IS NULL) 
OR(in_merch_level IS NULL) OR(in_loc_level IS NULL) OR(in_time_level IS NULL) OR(in_time_id IS NULL)
OR(in_merch_level<0) OR(in_merch_level>10) 
OR(in_loc_level<0) OR(in_loc_level>4)
OR(in_time_level<47) OR(in_time_level>51) 
THEN
BEGIN
    RAISE_APPLICATION_ERROR (-20001, 'Invalid parameter values.');
END;
END IF;

n_sqlnum := 2100;
SELECT COUNT(*) INTO t_base_flg
FROM maxdata.v_base_level
WHERE in_merch_level=(base_merch_level-10) 
AND in_loc_level=base_location_level
AND in_time_level=base_time_level ;
IF (t_base_flg>0) THEN
    RAISE_APPLICATION_ERROR (-20001, 'At least one dimension must be above the base level.');
END IF;

t_time_level:=TO_CHAR(in_time_level - 46);
out_simple_col_list := NULL;
out_aggr_col_list := NULL;
t_error_level := 'error';

n_sqlnum := 2100;
SELECT value_1 INTO t_dim_order 
FROM maxapp.userpref
WHERE max_user_id=-1 AND key_1='AGG_DIMENSION_ORDER';

n_sqlnum := 3000;
SELECT table_id INTO t_tbl_id
FROM datamgr.fact_table
WHERE TRIM(table_name) = in_table_nm;

BEGIN
DECLARE CURSOR c_col IS
    SELECT column_id,column_name
    FROM datamgr.column_map 
    WHERE table_id=t_tbl_id 
    AND( UPPER(column_name) 
            NOT IN('MERCH_LEVEL','MERCH_ID','LOCATION_LEVEL','LOCATION_ID','TIME_LEVEL','TIME_ID') )
    AND(   ( in_aggr_hier!='C' )--if 'M','L','T', then get rules for ALL columns--
        OR ( UPPER(column_name) --if 'C', then get rules for only the #COUNT columns--
            IN(SELECT column_name FROM user_tab_columns WHERE table_name=t_count_tbl) ) 
       ) 
    ORDER BY column_id;
BEGIN
FOR c1 in c_col LOOP
    n_sqlnum := 7000;
    t_col_name:=UPPER(c1.column_name);
    t_col_name:=REPLACE(t_col_name,' ',''); --remove any blanks--
    BEGIN
    IF t_dim_order='ML' THEN
        SELECT rule,db_internal_func INTO t_rule,t_db_func
        FROM(
            SELECT time_level,merch_level,loc_level,agg_id
            FROM datamgr.col_agg_rule c
            WHERE table_id=t_tbl_id
            AND column_id=c1.column_id
                AND time_level IN(in_time_level,0)
                AND merch_level IN(in_merch_level,0)
                AND loc_level IN(in_loc_level,0)
            ORDER BY time_level DESC,merch_level DESC,loc_level DESC) CAR --T-M-L sort--
        JOIN datamgr.agg_rule AR ON(CAR.agg_id=AR.agg_id)    
        WHERE ROWNUM=1;
    ELSE--t_dim_order='LM'
        SELECT rule,db_internal_func INTO t_rule,t_db_func
        FROM(
            SELECT time_level,merch_level,loc_level,agg_id
            FROM datamgr.col_agg_rule c
            WHERE table_id=t_tbl_id
            AND column_id=c1.column_id
                AND time_level IN(in_time_level,0)
                AND merch_level IN(in_merch_level,0)
                AND loc_level IN(in_loc_level,0)
            ORDER BY time_level DESC,loc_level DESC,merch_level DESC) CAR --T-L-M sort--
        JOIN datamgr.agg_rule AR ON(CAR.agg_id=AR.agg_id)    
        WHERE ROWNUM=1;
    END IF;    
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        t_error_msg:='Aggregation rule not found for '||t_col_name||',id:'||c1.column_id;
        maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
        RAISE_APPLICATION_ERROR(-20001,t_error_msg); 
    END;
    n_sqlnum := 9000;
    t_rule:=UPPER(t_rule);

    CASE
      WHEN SUBSTR(t_db_func,1,3)='*UN' THEN
        n_sqlnum := 9100;
        t_error_msg:='Specified aggregation rule not implemented yet: '||t_rule|| ' for '||t_col_name||',id:'||c1.column_id;
        RAISE_APPLICATION_ERROR(-20001,t_error_msg); 

      WHEN t_rule='BOP' THEN
        n_sqlnum := 9200;
        IF in_aggr_hier IN('T','C') THEN
          t_rule_col:='SUM( (CASE time_id WHEN '||+(in_child_first_id)||' THEN 1 ELSE 0 END)*'||t_col_name||')';
        ELSE 
          t_rule_col:='SUM('||t_col_name||')';
        END IF;
      
      WHEN t_rule='EOP' THEN
        n_sqlnum := 9300;
        IF in_aggr_hier IN('T','C') THEN
          t_rule_col:='SUM( (CASE time_id WHEN '||+(in_child_last_id)||' THEN 1 ELSE 0 END)*'||t_col_name||')';
        ELSE 
          t_rule_col:='SUM('||t_col_name||')';
        END IF;
       
      WHEN SUBSTR(t_rule,1,5)='FUNC(' THEN
         n_sqlnum := 9400;
         v_sql:=REPLACE(t_db_func,'[M]',in_merch_level);
         v_sql:=REPLACE(t_db_func,'[L]',in_loc_level);
         v_sql:=REPLACE(t_db_func,'[T]',t_time_level);
         v_sql:=REPLACE(v_sql,'[ID]',in_time_id);
         EXECUTE IMMEDIATE v_sql INTO t_rule_col;

      WHEN t_db_func IS NOT NULL THEN
         n_sqlnum := 9500;
         t_rule_col:=REPLACE(t_db_func,'()','('||t_col_name||')');

      ELSE
         n_sqlnum := 9600;
         t_rule_col:=t_rule||'('||t_col_name||')';
          
    END CASE;             

    n_sqlnum := 10000;
    IF out_simple_col_list IS NULL THEN
    BEGIN
        out_simple_col_list := t_col_name;
        out_aggr_col_list := t_rule_col;
    END;
    ELSE
    BEGIN
        out_simple_col_list := out_simple_col_list||','||t_col_name;
        out_aggr_col_list := out_aggr_col_list||','||t_rule_col;
    END;
    END IF;

END LOOP; -- cursor loop
END; -- begin after declare 
END; -- declare

n_sqlnum := 11000;
COMMIT;


EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
        t_error_msg := SQLERRM || ' (' || t_call ||
                ', SQL#:' || n_sqlnum || ')';

    ROLLBACK;

    t_rule:=COALESCE(t_rule, 'NULL');
    t_rule_col:=COALESCE(t_rule_col, 'NULL');
    out_simple_col_list:=COALESCE(out_simple_col_list, 'NULL');
    out_aggr_col_list:=COALESCE(out_aggr_col_list, 'NULL');
    
    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    maxdata.p_log (t_proc_name, t_error_level, t_col_name||','||t_rule||','||t_rule_col, v_sql, n_sqlnum);
    maxdata.p_log (t_proc_name, t_error_level, out_simple_col_list, NULL, n_sqlnum);
    maxdata.p_log (t_proc_name, t_error_level, out_aggr_col_list, NULL, n_sqlnum);
    COMMIT;
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
