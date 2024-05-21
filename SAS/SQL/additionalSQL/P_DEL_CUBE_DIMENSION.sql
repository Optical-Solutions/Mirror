--------------------------------------------------------
--  DDL for Procedure P_DEL_CUBE_DIMENSION
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DEL_CUBE_DIMENSION" (
	in_cube_id	NUMBER,
	in_future1	NUMBER,		-- placeholder. Pass in -1.
	in_future2	NUMBER,		-- placeholder. Pass in -1.
	in_future3	VARCHAR2	-- placeholder. Pass in NULL.
) AS
/*----------------------------------------------------------------------------
$Log: 2302_p_del_cube_dimension.sql,v $
Revision 1.7  2007/06/19 14:39:14  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2005/09/02 16:17:39  anchan
No index for T_CUBE_4KEY in Oracle.

==================================
Change History:
6.0.0-000 04/21/05	Andy	Created

Description:
Deletes rows of the specified CUBE_ID from all 2KEY and 4KEY tables.
------------------------------------------------------------------------------*/

n_sqlnum 	        NUMBER(10,0);
t_proc_name        	VARCHAR2(32) 		:= 'p_del_cube_dimension';
t_error_level      	VARCHAR2(6) 		:= 'info';
t_call            	VARCHAR2(1000);
v_sql              	VARCHAR2(1000) 		:= NULL;
t_sql2			VARCHAR2(255);
t_sql3			VARCHAR2(255);
t_cnt			NUMBER(10,0);
t_err_msg		VARCHAR2(255);
t_str_null		VARCHAR2(255)		:= NULL;
t_int_null		NUMBER(10)		:= NULL;

BEGIN
n_sqlnum := 1000;

t_call := t_proc_name || ' ( ' ||
	NVL(in_cube_id, -1) ||','||
	NVL(in_future1, -1) ||','||
	NVL(in_future2, -1) ||','||
	NVL(in_future3, ' ') ||')' ;

--maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;

--Clean-up the rows from WORKING tables:--
n_sqlnum := 2000;
DELETE FROM maxdata.t_cube_pwid
WHERE cube_id=in_cube_id;

n_sqlnum := 3000;
DELETE FROM maxdata.t_cube_merch
WHERE cube_id=in_cube_id;

n_sqlnum := 4000;
DELETE FROM maxdata.t_cube_loc
WHERE cube_id=in_cube_id;

n_sqlnum := 5000;
DELETE FROM maxdata.t_cube_time
WHERE cube_id=in_cube_id;

n_sqlnum := 6000;
--No index for T_CUBE_4KEY(hash-partitioned) in Oracle, but needed in UDB,SS:
DELETE FROM maxdata.t_cube_4key
WHERE cube_id=in_cube_id;

COMMIT;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. NOT necessarily related WITH the CURRENT error';
			t_sql3 := SUBSTR(v_sql,1,255);
			maxdata.Ins_Import_Log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
			--COMMIT;
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := SUBSTR(v_sql,1,255);
		t_sql3 := SUBSTR(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, t_int_null);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DEL_CUBE_DIMENSION" TO "MADMAX";
