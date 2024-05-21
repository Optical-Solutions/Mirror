--------------------------------------------------------
--  DDL for Procedure P_WL_FIX_KPIDV
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_WL_FIX_KPIDV" (
    in_KPI_DV_ID           NUMBER, --new id; must be between 1000 AND 1999.
    in_DV_ID               NUMBER,
    in_TIME_LEVEL_NO       NUMBER,
    in_TIME_ID             NUMBER,
    in_AGGREGATE_FLG       NUMBER,
    in_TIME_SHIFT_FLG      NUMBER,
    in_PLAN_ID             NUMBER,
    in_PLANVERSION_ID      NUMBER,
    in_START_DT            VARCHAR2, --must be in MM/DD/YYYY format or '' if none.
    in_OFFSET_PERIOD_CNT   NUMBER,
    in_DATAVERSION_NM      VARCHAR2,
    in_action_cd           VARCHAR2 --either PREVIEW(only validate 9keys) or EXECUTE--
) AS

/*
$Log: 2222_p_wl_fix_kpidv.sql,v $
Revision 1.1.2.2  2008/10/23 14:16:19  anchan
Uncommented COMMIT

Revision 1.1.2.1  2008/04/15 19:56:18  anchan
FIXID S0450686: add/modify the kpi_dv_id with specified 9key-values

--DESCRIPTION--
**Intended for use by Professional Services folks only**

This procedure is used to logically assign a new kpi_dv_id to an existing kpi_dv_id.

Normally, changing a kpi_dv_id of the 9key combination is practically impossible
if the 9key record already exists, because of all the referential integrity constraints.

If the matching 9key record already exists, the new id is inserted and all the referencing
tables are updated with the new id, and the old id is finally deleted.

If the 9key combination doesn't exist yet,it simply inserted with the specified id.
*/

n_sqlnum            NUMBER(10);
t_proc_name         VARCHAR2(32):= 'p_wl_fix_kpidv';
t_error_level       VARCHAR2(6):= 'info';
t_call              VARCHAR2(1000);
v_sql               VARCHAR2(1000):= NULL;
t_sql2              VARCHAR2(255);
t_sql3              VARCHAR2(255);

t_old_id            NUMBER(10);
t_field_id          NUMBER(10);
t_exists_flg        NUMBER(10);
t_start_dt          DATE;


BEGIN
n_sqlnum := 1000;
t_call := t_proc_name || ' ( ' ||
    in_KPI_DV_ID||','||
    in_DV_ID||','||
    in_TIME_LEVEL_NO||','||
    in_TIME_ID||','||
    in_AGGREGATE_FLG||','||
    in_TIME_SHIFT_FLG||','||
    in_PLAN_ID||','||
    in_PLANVERSION_ID||','||
    in_START_DT||','||
    in_OFFSET_PERIOD_CNT||','||
    in_DATAVERSION_NM||','||
    in_action_cd||
	' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

n_sqlnum := 1100;
IF NOT in_KPI_DV_ID BETWEEN 1000 AND 1999 THEN
	RAISE_APPLICATION_ERROR(-20001,'The new KPI_DV_ID must be between 1000 and 1999.');
END IF;

n_sqlnum := 1200;
IF UPPER(in_action_cd) NOT IN('PREVIEW','EXECUTE') THEN
	RAISE_APPLICATION_ERROR(-20001,'Action must be either PREVIEW or EXECUTE.');
END IF;


n_sqlnum := 1300;
--Check if the dv_id is valid--
SELECT COUNT(*) INTO t_exists_flg
FROM maxapp.dataversion
WHERE  dv_id=in_DV_ID;

IF t_exists_flg=0 THEN
	RAISE_APPLICATION_ERROR(-20001,'The DV_ID does not exist:'||TO_CHAR(in_dv_id));
END IF;

n_sqlnum := 2000;
--remove any blanks and convert to date--
t_start_dt:=TO_DATE(REPLACE(in_START_DT,' ',''),'MM/DD/YYYY'); --needed later anyway, since INSERT hangs if TO_DATE function is embedded?--

n_sqlnum := 2100;
--Check if an old id with the same 9-key combination already exists--
SELECT MAX(kpi_dv_id) INTO t_old_id
FROM maxdata.wlkd_kpi_dataversion
WHERE dv_id=in_DV_ID
AND time_level_no=in_TIME_LEVEL_NO
AND time_id=in_TIME_ID
AND aggregate_flg=in_AGGREGATE_FLG
AND time_shift_flg=in_TIME_SHIFT_FLG
AND plan_id=in_PLAN_ID
AND planversion_id=in_PLANVERSION_ID
AND COALESCE(start_dt,SYSDATE)=COALESCE(t_start_dt,SYSDATE)
AND offset_period_cnt=in_OFFSET_PERIOD_CNT;

n_sqlnum := 2200;
IF(t_old_id<100)THEN
    RAISE_APPLICATION_ERROR(-20001,'The old KPI_DV_ID='||TO_CHAR(t_old_id)
        ||' cannot be replaced by new KPI_DV_ID='||TO_CHAR(in_kpi_dv_id)
        ||'. It is a system or reserved record.' );
END IF;


--IF t_old_id BETWEEN 1000 AND 1999 THEN
--    RAISE_APPLICATION_ERROR?
--END IF;

n_sqlnum := 3000;
IF(t_old_id<>in_kpi_dv_id)THEN
BEGIN
    n_sqlnum := 3100;
    --Check if the new id already exists--
    SELECT COUNT(*) INTO t_exists_flg
    FROM maxdata.wlkd_kpi_dataversion
    WHERE  kpi_dv_id=in_KPI_DV_ID;

    IF t_exists_flg>0 THEN
        RAISE_APPLICATION_ERROR(-20001,'The new KPI_DV_ID already exists:'||TO_CHAR(in_kpi_dv_id));
    END IF;

    n_sqlnum := 3200;
    SELECT MAX(field_id) INTO t_field_id
    FROM maxapp.calcrules
    WHERE dv_id=t_old_id; -- in calcrules, dv_id is really kpi_dv_id--

    IF(t_field_id IS NOT NULL)THEN
	    RAISE_APPLICATION_ERROR(-20001,'The old KPI_DV_ID='||TO_CHAR(t_old_id)
            ||' cannot be replaced by new KPI_DV_ID='||TO_CHAR(in_kpi_dv_id)
            ||'. It is still used by CALCRULES=:'||TO_CHAR(t_field_id) );
    END IF;

    n_sqlnum := 3300;
    --Temporarily change one of the 9 keys, to move it out of the way--
    UPDATE maxdata.wlkd_kpi_dataversion
    SET time_id=-111111
    WHERE kpi_dv_id=t_old_id;
END;
END IF;

IF(COALESCE(t_old_id,0)<>in_kpi_dv_id)THEN
    n_sqlnum := 4000;
    INSERT INTO maxdata.wlkd_kpi_dataversion
        (kpi_dv_id,dv_id,time_level_no,time_id,aggregate_flg,time_shift_flg,plan_id,
        planversion_id,start_dt,offset_period_cnt,dataversion_nm)
    VALUES
        (in_KPI_DV_ID,in_DV_ID,in_TIME_LEVEL_NO,in_TIME_ID,in_AGGREGATE_FLG,in_TIME_SHIFT_FLG,in_PLAN_ID,
        in_PLANVERSION_ID,t_start_dt,in_OFFSET_PERIOD_CNT,in_DATAVERSION_NM);
ELSE --t_old_id=in_kpi_dv_id--
    n_sqlnum := 4100;
    UPDATE maxdata.wlkd_kpi_dataversion
    SET dataversion_nm=in_DATAVERSION_NM
    WHERE kpi_dv_id=t_old_id;
END IF;

n_sqlnum := 6000;
IF (UPPER(in_action_cd)<>'EXECUTE') THEN
    ROLLBACK;
ELSE
BEGIN
    IF(t_old_id IS NOT NULL)THEN
    BEGIN
        n_sqlnum := 6100;
        UPDATE maxdata.cl_hist_status
        SET kpi_dv_id=in_KPI_DV_ID
        WHERE kpi_dv_id=t_old_id;
        n_sqlnum := 6200;
        UPDATE maxdata.wlkv_kpi_variance
        SET to_kpi_dv_id=in_KPI_DV_ID
        WHERE to_kpi_dv_id=t_old_id;
        n_sqlnum := 6300;
        UPDATE maxdata.wlkv_kpi_variance
        SET from_kpi_dv_id=in_KPI_DV_ID
        WHERE from_kpi_dv_id=t_old_id;
        n_sqlnum := 6400;
        UPDATE maxdata.wlkf_kpi_field
        SET kpi_dv_id=in_KPI_DV_ID
        WHERE kpi_dv_id=t_old_id;
        n_sqlnum := 6500;
        UPDATE maxdata.wltd_template_dataversion
        SET kpi_dv_id=in_KPI_DV_ID
        WHERE kpi_dv_id=t_old_id;
        n_sqlnum := 6600;
        UPDATE maxdata.wldt_dataversion_time
        SET kpi_dv_id=in_KPI_DV_ID
        WHERE kpi_dv_id=t_old_id;

        n_sqlnum := 6900;
        DELETE FROM maxdata.wlkd_kpi_dataversion
        WHERE kpi_dv_id=t_old_id;
    END;
    END IF;

    n_sqlnum := 7000;
    COMMIT;
END;
END IF;

EXCEPTION
	WHEN OTHERS THEN
		ROLLBACK;

		IF v_sql IS NOT NULL THEN
			t_error_level := 'info';
			t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
			t_sql3 := substr(v_sql,1,255);
			maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, -1);
		END IF;

		-- Log the error message
		t_error_level := 'error';
		v_sql := SQLERRM || ' (' || t_call ||
				', SQL#:' || n_sqlnum || ')';

		t_sql2 := substr(v_sql,1,255);
		t_sql3 := substr(v_sql,256,255);
		maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, -1);
		--COMMIT;

		RAISE_APPLICATION_ERROR(-20001,v_sql);

END;

/
