--------------------------------------------------------
--  DDL for Procedure P_COMPOSE_QUERY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COMPOSE_QUERY" (
	in_query_type		VARCHAR2,
	in_cube_id 		NUMBER, 	-- cube id ,(-1) for NULL
	in_kpi_dv_id 		NUMBER, 	-- kpi dataversion id for time ,(-1) for NULL
	in_hashkey_id		NUMBER,		-- common//hashkey; usually pw_id,or fcast_ver_id; (-1) for NULL
	in_tablename		VARCHAR2,	-- A FACT or MPLAN-split tablename.
	in_future1		NUMBER,		-- (-1)
	in_future2		NUMBER,		-- (-1)
	in_future3		NUMBER,		-- (-1)
	in_future4		NUMBER,		-- (-1)
	in_debug_flg    	NUMBER,		-- (0)
	out_dml		OUT	VARCHAR2,
	out_join	OUT	VARCHAR2,
	out_where	OUT	VARCHAR2,
	out_option  OUT VARCHAR2,	-- Reserved for SS...
	out_future1	OUT	VARCHAR2
) AS
/*-------------------------------------------------------------------------------
$Log: 2141_p_compose_query.sql,v $
Revision 1.5.20.3  2010/02/19 19:54:57  anchan
FIXID S0645908: Added missing query_option statement.

Revision 1.5.20.2  2010/02/01 21:46:15  anchan
FIXID S0614606: A conditional hint depends on the number of 4KEY counts, specified using square brackets [ ].

:
:
Revision 1.8  2005/12/08 16:52:41  anchan
Break out of the loop on first check if the row does not exist.

Revision 1.5  2005/12/07 20:51:58  anchan
Check and wait for truncate of T_CUBE_ tables to finish.

Revision 1.4  2005/12/05 17:32:49  anchan
Changed to handle FORECAST_CUBE_SINGLE

Revision 1.3  2005/10/04 19:08:53  anchan
Fixed replace with in_pw_id

Revision 1.2  2005/09/27 15:16:39  anchan
Refactored and removed hard-coded QUERY_TYPE

V6.1
============================================
NOTE: Must have been granted by the SYS user:
GRANT EXECUTE on sys.dbms_lock to maxdata;

--------------------------------------------------------------------------------*/

n_sqlnum 	        	NUMBER(10,0);
t_proc_name        		VARCHAR2(32) 		:= 'p_compose_query';
t_error_level      		VARCHAR2(6) 		:= 'info';
t_call            		VARCHAR2(1000);
v_sql              		VARCHAR2(1000) 		:= NULL;
t_sql2				VARCHAR2(255);
t_sql3				VARCHAR2(255);

t_dml				VARCHAR2(2000);
t_join				VARCHAR2(2000);
t_where				VARCHAR2(2000);
t_option			VARCHAR2(255);
t_time_clause			VARCHAR2(8000);
t_cube_busy_flg			NUMBER(1);
t_wait_cnt			NUMBER(10);
BEGIN
n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
	COALESCE(in_query_type, 'NULL')  || ',' ||
	COALESCE(in_cube_id, -1) || ',' ||  	-- COALESCE(int, 'NULL') returns error because of diff datatype.
	COALESCE(in_kpi_dv_id, -1) || ',' ||
	COALESCE(in_hashkey_id, -1) || ',' ||
	COALESCE(in_tablename, 'NULL')  || ',' ||
	COALESCE(in_future1, -1) || ',' ||
	COALESCE(in_future2, -1) || ',' ||
	COALESCE(in_future3, -1) || ',' ||
	COALESCE(in_future4, -1) || ',' ||
	COALESCE(in_debug_flg, -1) || ',' ||
	'OUT out_dml,out_join,out_where,out_option,out_future1' ||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

BEGIN
n_sqlnum:=2000;
SELECT query_dml,query_join,query_where,query_option
INTO t_dml,t_join,t_where,t_option
FROM maxdata.config_query_syntax
WHERE query_type = UPPER(in_query_type)
AND custom_syntax_flg =(SELECT MAX(custom_syntax_flg)
			FROM maxdata.config_query_syntax
			WHERE query_type = UPPER(in_query_type));
EXCEPTION
	WHEN NO_DATA_FOUND THEN
		RAISE_APPLICATION_ERROR (-20001, 'Not a VALID query type.');
END;

out_dml := t_dml;
out_join := t_join;
out_where := t_where;
out_option := t_option;

n_sqlnum:=3000;
IF(INSTR(t_dml,'[')>0)THEN
BEGIN
    DECLARE
        t_op_cd CHAR(1);i INTEGER; j INTEGER;
        t_cube_m_cnt INTEGER; t_cube_l_cnt INTEGER; t_4key_cutoff INTEGER;
    BEGIN
        n_sqlnum:=3100;
        SELECT COUNT(*) INTO t_cube_m_cnt FROM maxdata.t_cube_merch WHERE cube_id=in_cube_id;
        SELECT COUNT(*) INTO t_cube_l_cnt FROM maxdata.t_cube_loc WHERE cube_id=in_cube_id;

        n_sqlnum:=3200;
        --Conditional hint must be of format: [HINT(....)>nnnn], where nnnn is 4key cutoff count--
        j:=INSTR(t_dml,']');
        i:= INSTR(t_dml,'>'); t_op_cd:='>';
        IF(i=0)THEN
           i:= INSTR(t_dml,'<'); t_op_cd:='<';
        END IF;
        t_4key_cutoff:= SUBSTR(t_dml,i+1,j-i-1);

        n_sqlnum:=3300;
        IF((t_op_cd='>')AND(t_cube_m_cnt*t_cube_l_cnt>t_4key_cutoff))
        OR((t_op_cd='<')AND(t_cube_m_cnt*t_cube_l_cnt<t_4key_cutoff))THEN
            t_dml:= TRANSLATE(t_dml,'[]','  ');
        END IF;

    END;--ORA-specific code for optional hint--
END;
END IF;

n_sqlnum:=4000;
IF (INSTR(t_where,'%CUBE_ID')>0) THEN
BEGIN
	IF (COALESCE(in_cube_id,-1) = -1) THEN
		RAISE_APPLICATION_ERROR (-20001, 'Cube_ID must be supplied.');
	ELSE
	BEGIN
		out_where := REPLACE (out_where,'%CUBE_ID',CAST(in_cube_id AS VARCHAR2));
	END;
	END IF;
END;
END IF;

n_sqlnum:=5000;
IF (INSTR(t_where,'%HASHKEY_ID')>0) THEN
BEGIN
	IF (COALESCE(in_hashkey_id,-1) = -1) THEN
		RAISE_APPLICATION_ERROR (-20001, 'Common/Hashkey_ID must be supplied.');
	ELSE
	BEGIN
		out_where := REPLACE (out_where,'%HASHKEY_ID',CAST(in_hashkey_id AS VARCHAR2));
	END;
	END IF;
END;
END IF;

n_sqlnum:=6000;
IF (INSTR(t_where,'%TIME_CLAUSE')>0) THEN
BEGIN
	maxdata.p_gen_time_inclause(in_cube_id,in_kpi_dv_id,-1,-1,-1,t_time_clause);
	out_where := REPLACE (out_where,'%TIME_CLAUSE',t_time_clause);
END;
END IF;

n_sqlnum:=7000;
IF (INSTR(t_join,'%MAIN_TABLE')>0) THEN
BEGIN
	IF (in_tablename IS NULL) THEN
		RAISE_APPLICATION_ERROR (-20001, 'Tablename must be supplied.');
	ELSE
	BEGIN
		out_dml	:= REPLACE(t_dml,'%MAIN_TABLE',in_tablename);
		out_join := REPLACE(t_join,'%MAIN_TABLE',in_tablename);
	END;
	END IF;
END;
END IF;

n_sqlnum:=8000;
--Loop here while the T_CUBE_ tables are being truncated.
--Break out of the loop only if the row does not exist:
t_cube_busy_flg:=1;
t_wait_cnt:=1;
WHILE (t_cube_busy_flg=1) AND (t_wait_cnt<=36) LOOP
BEGIN
	SELECT COUNT(*) INTO t_cube_busy_flg
	FROM maxdata.WLOOw_object_operation
	WHERE cube_id =- 1000 AND worksheet_template_id =- 1000;

	IF t_cube_busy_flg=1 THEN
	BEGIN
		  sys.dbms_lock.sleep(5);
	END;
	END IF;

	t_wait_cnt := t_wait_cnt + 1;
END;
END LOOP;

IF (t_cube_busy_flg>0) THEN
	RAISE_APPLICATION_ERROR(-20001,'Timed out while waiting for T_CUBE_ tables to be truncated.');
END IF;

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

  GRANT EXECUTE ON "MAXDATA"."P_COMPOSE_QUERY" TO "MADMAX";
