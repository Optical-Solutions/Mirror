--------------------------------------------------------
--  DDL for Procedure P_UPDATE_CLOSED_STORE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPDATE_CLOSED_STORE" AS
/*----------------------------------------------------------------------

Change History

V5.6.1
5.6.1-047 01/20/05 Diwakar	#17008. Initial Entry. Assign closed store planograms
				to dummy section and insert all dummy section planograms
				back to maxdata.pogmaster.


Description:

This procedure is to be used to assign closed store planograms to dummy planogram.
This procedure is called from maxdata.p_pog_approval:
--------------------------------------------------------------------------------*/



n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 			:= 'p_update_closed_store';
t_error_level      	VARCHAR2(6) 			:= 'info';
t_call            	VARCHAR2(4000);
v_sql              	VARCHAR2(4000) 			:= NULL;
t_sql2			VARCHAR2(4000);
t_sql3			VARCHAR2(4000);
t_col_value		VARCHAR2(100);
t_level			NUMBER(10,0);
t_entity		NUMBER(10,0);
t_increment		NUMBER(10,0);
t_cnt			NUMBER(10,0);
t_lv7mast_id		NUMBER(10,0);
t_lv7loc_id		NUMBER(10,0);
t_pog_id		NUMBER(10,0);
t_errcode		NUMBER(10,0);
t_errmsg		VARCHAR2(1000);
v_sql_extra		VARCHAR2(4000);

table_exists EXCEPTION;
table_not_exists EXCEPTION;
PRAGMA EXCEPTION_INIT(table_exists, -955);
PRAGMA EXCEPTION_INIT(table_not_exists, -942);


BEGIN

-- Delete the log records that belong to this procedure
-- Keep the recent 20 logs

DELETE FROM maxdata.import_log
WHERE log_id = t_proc_name
AND log_nbr2 <
	(select max(log_nbr2) - 20
	from maxdata.import_log
	where log_id = t_proc_name);

-- Log the parameters of the procedure
n_sqlnum := 2000;
t_call := t_proc_name || ' ' ;

maxdata.ins_import_log  (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

n_sqlnum := 2000;

-- See if there is any planograms belong to a closed store.. if there isn't one we insert dummy one.
SELECT count(*) INTO t_cnt from maxdata.lv7loc where lv7loc_userid = 'CLOSED_STORE_POG';
IF t_cnt = 0 THEN

	t_level := 7;
	t_entity := 2;
	t_increment := 1;

	n_sqlnum := 3000;

	maxapp.p_get_next_key (t_level,t_entity,t_increment,t_lv7loc_id ,t_errmsg );


	n_sqlnum := 4000;
	SELECT  lv7mast_id INTO t_lv7mast_id FROM maxdata.lv7mast where UPPER(lv7mast_userid)='DEFAULT';

	n_sqlnum := 7000;
	INSERT INTO maxdata.lv7loc
		(lv7loc_id, last_update, lv7loc_userid, lv7mast_id, lv4loc_id, record_type)
		VALUES
		(t_lv7loc_id, SYSDATE,'CLOSED_STORE_POG', t_lv7mast_id,-1,'M');

ELSE

	n_sqlnum := 8000;

	 SELECT lv7loc_id INTO t_lv7loc_id FROM maxdata.lv7loc where lv7loc_userid = 'CLOSED_STORE_POG';
END IF;

v_sql := Null;
n_sqlnum := 9000;
v_sql_extra := Null;

-- Select pogmaster records that belong to closed store, making pog_master_id *negative* and inserting into pogmaster


DECLARE CURSOR c_wrksht_cols IS
	SELECT UPPER(column_name) column_name FROM user_tab_columns
	WHERE table_name = 'POGMASTER'
	ORDER BY column_name;
	BEGIN
	FOR c1 IN c_wrksht_cols LOOP
		IF v_sql IS NULL  THEN
			v_sql := 'INSERT INTO maxdata.POGMASTER ( ';
		ELSE
			v_sql := v_sql ||',';
		END IF;
		v_sql := v_sql || c1.column_name;

		t_col_value := 	CASE c1.column_name
						WHEN 'POG_MASTER_ID' then '-' || 'POG.'||  c1.column_name
						WHEN 'POG_ACTUAL_START' THEN ' LOC4.close_date'
						WHEN 'POG_MODEL_ID' THEN TO_CHAR(t_lv7loc_id)
						ELSE 'POG.'||  c1.column_name
					        END ;

		IF v_sql_extra IS NULL THEN
			v_sql_extra := ' ) SELECT ' ;
		ELSE
			v_sql_extra := v_sql_extra ||',';
		END IF;
		v_sql_extra := v_sql_extra || t_col_value;
	END LOOP;
	END;



v_sql := v_sql  || v_sql_extra ||
		' FROM maxdata.pogmaster POG, MAXDATA.LV4LOC LOC4' ||
		' WHERE POG.pog_lv4loc_id = LOC4.LV4LOC_ID AND LOC4.close_date <= SYSDATE ' ||
		' AND pog_model_id <>  ' || t_lv7loc_id  ||
		' AND (pog_end_date > SYSDATE or pog_end_date IS NULL)' ||
		' AND approval_status = 1';




--PRINT V_SQL
n_sqlnum := 10000;
EXECUTE IMMEDIATE (v_sql);
t_increment := SQL%ROWCOUNT ;

t_level := 23;
t_entity := 4096;

n_sqlnum := 11000;
maxapp.p_get_next_key (t_level,t_entity,t_increment,t_pog_id ,t_errmsg );

n_sqlnum := 12000;


UPDATE MAXDATA.POGMASTER SET -- this update is just for SQL server . in oracle use (+rownum - 1) without variable
pog_master_id = t_pog_id + ROWNUM - 1
WHERE POG_MASTER_ID < 0;
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
		COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);

END;


/

  GRANT EXECUTE ON "MAXDATA"."P_UPDATE_CLOSED_STORE" TO "MADMAX";
