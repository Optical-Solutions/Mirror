--------------------------------------------------------
--  DDL for Procedure P_AGG_CHK_COL_RULE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_AGG_CHK_COL_RULE" (
    in_table_nm   VARCHAR2,
    in_debug_flg    NUMBER  -- debug flag, 0=off, other=on, 2=don't raise error but just log to import_log
) AS

/* ----------------------------------------------------------------------------
S0583005: Remove paragraph from p.244, "About count column verification"
Change History:

$Log: 5250_IDA_p_agg_chk_col_rule.sql,v $
Revision 1.1.2.1.2.4  2009/06/08 20:38:21  anchan
FIXID S0583005: Remove the unnecessarily restrictive check of count-like columns

Revision 1.1.2.1.2.3  2009/05/13 20:02:16  anchan
PERFORMANCE: use separate work tables for each of the M-L-T aggregation dimensions.

Revision 1.1.2.1.2.2  2009/04/22 16:18:04  anchan
FIXID S0567466: allow valid multiple agg_rules for the same column.

Revision 1.1.2.1.2.1  2009/03/25 16:24:30  anchan
FIXID S0567466: moved revamped code to check the ambiguous agg_rules from p_agg_col_rule to here

Revision 1.1.2.1  2008/11/26 17:30:15  anchan
FIXID : BASELINE check-in


Usage: Used by DB only (In-Database-Aggregation), not by the appl.

Description:

Validate the aggr rules defined in DATAMGR.COL_AGG_RULE.
---------------------------------------------------------------------------- */

n_sqlnum            NUMBER(10)    := 1000;
t_proc_name         VARCHAR2(30)    := 'p_agg_chk_col_rule';
t_call              VARCHAR2(1000)    := 'p_agg_chk_col_rule(table_name, debug_flg)';
v_sql               VARCHAR2(4000)  := NULL;
t_error_level       VARCHAR2(6)     := 'info';
t_error_msg            VARCHAR2(4000);

t_agg_col_cnt        INT;
t_count_tbl_nm        VARCHAR2(128)    := in_table_nm||'#COUNT';
t_table_id          NUMBER(10);
t_column_id         NUMBER(10);
t_level             NUMBER(4);

BEGIN

n_sqlnum:=1000;
t_call := t_proc_name || ' ( ' ||
    in_table_nm || ',' ||
    maxdata.f_num_to_char(in_debug_flg) || 
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, NULL, n_sqlnum);

n_sqlnum := 2000;
SELECT MAX(table_id) INTO t_table_id
FROM datamgr.fact_table
WHERE table_name=in_table_nm;
IF(t_table_id IS NULL) THEN
    t_error_msg:='Table name '||in_table_nm||' not defined in DATAMGR.FACT_TABLE metadata.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;


n_sqlnum := 2100;
SELECT MIN(column_id) INTO t_column_id
FROM(--Check if any ambiguous agg rules exist for MERCHxLOC aggregation--
    SELECT column_id
    FROM datamgr.col_agg_rule 
    WHERE table_id=t_table_id
    GROUP BY column_id,time_level
    HAVING MAX(SIGN(merch_level)-SIGN(loc_level))=+1 --e.g.: T=50,M=1,L=0 --
       AND MIN(SIGN(merch_level)-SIGN(loc_level))=-1 --e.g.: T=50,M=0,L=1 --
    );
    
IF(t_column_id >0) THEN
    t_error_msg:='Ambiguous agg rules specified for TABLE_ID='||+(t_table_id)||', COLUMN_ID='||+(t_column_id)||'.'
                ||'  To skip checking(NOT recommended), set AGG_RULE_CHECK parameter to OFF in USERPREF.';
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END IF;


EXCEPTION
WHEN OTHERS THEN
    t_error_level:='error';
        t_error_msg := SQLERRM || ' (' || t_call ||
                ', SQL#:' || n_sqlnum || ')';

    ROLLBACK;

    maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
    COMMIT;
    RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/
