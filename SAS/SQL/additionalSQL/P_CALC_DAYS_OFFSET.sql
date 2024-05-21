--------------------------------------------------------
--  DDL for Procedure P_CALC_DAYS_OFFSET
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CALC_DAYS_OFFSET" 
        (in_kpi_dv_id     NUMBER,
         in_level_num     NUMBER, -- Expects 47-51
         in_level_id      NUMBER,
         out_sum_days     OUT NOCOPY NUMBER,
         out_53_week_flg  OUT NOCOPY NUMBER)
AS

n_sqlnum                 NUMBER(10)      := 100;
t_proc_name              VARCHAR2(30)    := 'p_calc_days_offset';
t_call                   VARCHAR2(1000);
v_sql                    VARCHAR2(4000)  := NULL;
t_sql2                   VARCHAR2(255);
t_sql3                   VARCHAR2(255);
t_error_level            VARCHAR2(6)     := 'info';
v_errmsg                 VARCHAR2(1000);
v_curr_offset            NUMBER(6)       := 0;
v_conv_dim_lvl_num       NUMBER;
v_dv_id_name             VARCHAR2(80);

BEGIN

n_sqlnum := 100;

t_call := t_proc_name||'('||in_kpi_dv_id||','||in_level_num||','||in_level_id||',:out_sum_days,:out_53_week_flg)';
maxdata.ins_import_log (t_proc_name,t_error_level, t_call, null, null, null);

--dbms_output.put_line(t_call);

-- All parameters must be not null
IF in_level_num IS NULL OR in_kpi_dv_id IS NULL OR in_level_id IS NULL THEN
        n_sqlnum := 110;
        v_sql := 'A null parameter was passed to'||t_proc_name||'; '||n_sqlnum;
        RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;

-- Level must be valid
IF in_level_num < 47 OR in_level_num > 51 THEN
        n_sqlnum := 120;
        v_sql := 'The level passed to '||t_proc_name||' is out of range (level='||in_level_num||
                ') should be between 47 and 51; '||n_sqlnum;
        RAISE_APPLICATION_ERROR(-20001,v_sql);
END IF;

n_sqlnum := 200;
--dbms_output.put_line('in_dv_id='||in_kpi_dv_id);
SELECT timefactor,
       name
  INTO v_curr_offset,
       v_dv_id_name
  FROM maxapp.dataversion
 WHERE dv_id = (SELECT dv_id FROM maxdata.wlkd_kpi_dataversion WHERE kpi_dv_id=in_kpi_dv_id);

v_conv_dim_lvl_num := in_level_num -46;

n_sqlnum := 300;
-- Get the total days between cycles
-- Due to Oracle's restrictions on between statement's order
-- of values we need to check the offset value to prevent an error
IF v_curr_offset > 0 THEN -- FUTURE
        v_sql :=
        'SELECT SUM(days_in_period),COUNT(start_date_53week) '||
        '  FROM maxapp.lv1time lv1, '||
        '       (SELECT DISTINCT lv1time_lkup_id id '||
        '          FROM maxapp.lv5time '||
        '         WHERE lv'||v_conv_dim_lvl_num||'time_lkup_id = '||in_level_id||') dtm '||
        ' WHERE lv1time_lkup_id BETWEEN (SELECT lv1time_lkup_id '||
        '                                  FROM maxapp.lv1time '||
        '                                 WHERE INSTR(dtm.id,lv1time_lkup_id) > 0) '||
        '                           AND (SELECT lv1time_lkup_id+'||v_curr_offset||
        '                                  FROM maxapp.lv1time '||
        '                                 WHERE INSTR(dtm.id,lv1time_lkup_id) > 0) '||
        '   AND lv1time_lkup_id != dtm.id';

        n_sqlnum := 310;
        --dbms_output.put_line('calc_off: (1a) TIME: '||n_sqlnum||', '||v_sql);

        n_sqlnum := 400;
        EXECUTE IMMEDIATE v_sql INTO out_sum_days,out_53_week_flg;
ELSIF v_curr_offset < 0 THEN -- PAST
        v_sql :=
        'SELECT SUM(days_in_period),COUNT(start_date_53week) '||
        '  FROM maxapp.lv1time lv1, '||
        '       (SELECT DISTINCT lv1time_lkup_id id '||
        '          FROM maxapp.lv5time '||
        '         WHERE lv'||v_conv_dim_lvl_num||'time_lkup_id = '||in_level_id||') dtm '||
        ' WHERE lv1time_lkup_id BETWEEN (SELECT lv1time_lkup_id+'||v_curr_offset||
        '                                  FROM maxapp.lv1time '||
        '                                 WHERE INSTR(dtm.id,lv1time_lkup_id) > 0) '||
        '                           AND (SELECT lv1time_lkup_id '||
        '                                  FROM maxapp.lv1time '||
        '                                 WHERE INSTR(dtm.id,lv1time_lkup_id) > 0)'||
        '   AND lv1time_lkup_id != dtm.id';

        n_sqlnum := 320;
        --dbms_output.put_line('calc_off: (1a) TIME: '||n_sqlnum||', '||v_sql);

        n_sqlnum := 400;
        EXECUTE IMMEDIATE v_sql INTO out_sum_days,out_53_week_flg;
ELSE -- TY
        n_sqlnum := 330;
        out_sum_days    := 0;
        out_53_week_flg := 0;
END IF;

-- dbms_output.put_line('out_sum_days='||out_sum_days);

IF out_sum_days IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001,'Unable to calculate days_in_period for id='||in_level_id||', level='||v_conv_dim_lvl_num||', dataversion='||v_dv_id_name||'. Check lv1time table.');
END IF;

-- out_53_week_flg should only be 0 or 1
IF out_53_week_flg > 1 THEN
        out_53_week_flg := 1;
END IF;

-- dbms_output.put_line('out_sum_days='||out_sum_days);
out_sum_days := SIGN(v_curr_offset)*out_sum_days;
-- dbms_output.put_line('out_sum_days='||out_sum_days);

EXCEPTION
        WHEN OTHERS THEN
                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := SUBSTR(v_sql,1,255);
                        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                END IF;
                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM||' ('||t_call||', SQL#:'||n_sqlnum||')';
                t_sql2 := SUBSTR(v_sql,1,255);
                t_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                COMMIT;
                RAISE_APPLICATION_ERROR(-20001,v_sql);
END p_calc_days_offset;

/
