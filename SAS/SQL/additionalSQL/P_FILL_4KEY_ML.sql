--------------------------------------------------------
--  DDL for Procedure P_FILL_4KEY_ML
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FILL_4KEY_ML" (
	in_cube_id NUMBER
) AS
/*
------------------------------------------------------------------------------
$Log: 2300_p_fill_4key_ml.sql,v $
Revision 1.12  2007/06/19 14:39:15  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.8  2006/09/25 14:52:21  anchan
A COMMIT added for better concurrency.

Revision 1.7  2006/06/02 13:57:08  anchan
Per Navin: Commented out the code to prevent too many rows being inserted into 4KEY table.

Revision 1.6  2006/02/17 21:19:47  anchan
More informative error message, related to #S0345001.


Revision 1.5  2006/01/03 18:07:57  anchan
Enforce the maximum expected cell count.

Revision 1.4  2005/12/05 17:05:11  anchan
Changed to allow unlimited number of cells

Revision 1.2  2005/08/11 13:28:58  anchan
Replaced parameter values with variables to make code UDB-compatible

=================================================================================
Description:
Populates the 4key table with a cartesian product of MERCH x LOC.
Cleans up the 4key table beforehand.
--------------------------------------------------------------------------------
*/

n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 			:= 'p_fill_4key_ml';
t_error_level      	VARCHAR2(6) 			:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 			:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_str_null		VARCHAR2(255)			:= NULL;
t_int_null		NUMBER(10)			:= NULL;

t_cnt			NUMBER(10);
t_max_cell_cnt		CONSTANT NUMBER(6):=250000; 	--200K+extra, according to nagarg, moali.
t_expected_cell_cnt	NUMBER(20);			--allow for a really big number?
t_expected_kpi_cnt	CONSTANT NUMBER(6):=300;	--according to nagarg, moali.

t_merch_cnt		NUMBER(10);
t_loc_cnt		NUMBER(10);


BEGIN

--Do not bother with logging parms.  This procedure is called thousands of times a day.

n_sqlnum := 1000;

IF (in_cube_id IS NULL) THEN
BEGIN
	v_sql := '"in_cube_id" cannot be null.';
	RAISE_APPLICATION_ERROR (-20001,v_sql);
END;
END IF;

n_sqlnum := 2000;
--First, remove any rows from previous run:
DELETE FROM maxdata.t_cube_4key
WHERE cube_id=in_cube_id;
COMMIT;
/*--Do NOT bother with checking the number of rows to be inserted into the 4KEY table--
n_sqlnum := 3000;
SELECT TO_NUMBER(COALESCE(property_value,default_value))/t_expected_kpi_cnt
INTO t_expected_cell_cnt
FROM maxdata.t_application_property
WHERE property_id=34;--"marketmax.plan.maxWSTolerance"= Merch x Loc x Time x Kpi

t_expected_cell_cnt := COALESCE(t_expected_cell_cnt,t_max_cell_cnt);


n_sqlnum := 3100;
SELECT COUNT(*) INTO t_merch_cnt
FROM maxdata.t_cube_merch
WHERE cube_id=in_cube_id;

n_sqlnum := 3200;
SELECT COUNT(*) INTO t_loc_cnt
FROM maxdata.t_cube_loc
WHERE cube_id=in_cube_id;

n_sqlnum := 3300;
IF (t_merch_cnt*t_loc_cnt > t_expected_cell_cnt) THEN
BEGIN
	v_sql := '[WL_TOO_MANY_CELLS] Potential number of cells('
	||TO_CHAR(t_merch_cnt)||'[M] x '||TO_CHAR(t_loc_cnt)||'[L]) exceeds the derived maximum of '
	||TO_CHAR(t_expected_cell_cnt)||', as specified in "marketmax.plan.maxWSTolerance" '
	||'(divided by 300, the expected KPI cnt).';
	RAISE_APPLICATION_ERROR (-20001,v_sql);
END;
END IF;
*/
n_sqlnum := 4000;
--Next, just do a cartesian join of the 2-KEY tables:
INSERT INTO maxdata.t_cube_4key
(cube_id,m_lev,m_id,l_lev,l_id)
SELECT m.cube_id,m.m_lev,m.m_id,l.l_lev,l.l_id
FROM maxdata.t_cube_merch m, maxdata.t_cube_loc l
WHERE m.cube_id=in_cube_id
AND l.cube_id=in_cube_id;

COMMIT;

EXCEPTION
  WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
			--COMMIT;
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_FILL_4KEY_ML" TO "MADMAX";
