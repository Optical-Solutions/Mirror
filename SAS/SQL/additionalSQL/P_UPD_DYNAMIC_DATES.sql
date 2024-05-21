--------------------------------------------------------
--  DDL for Procedure P_UPD_DYNAMIC_DATES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_DYNAMIC_DATES" (
    in_planworksheet_id  NUMBER
)
AS
/*
------------------------------------------------------------------------------
$Log: 2432_p_upd_dynamic_dates.sql,v $
Revision 1.1.2.1  2010/09/01 18:57:02  anchan
FIXID S0656726: No comment given. See defect documentation.

----
DESCRIPTION: Only for PA worksheets.  Does nothing if a non-PA worksheet.

    If the worksheet has a dynamic time template, the start_date and end_date
    are updated with the current dates.  A dimset_template is considered 'dynamic'
    if NUM_PERIODS > 0.


--NOTE: Event(level=45) MUST already have been added by the app--
--ALSO: Event member MUST already have been added by the app--
------------------------------------------------------------------------------
*/
    t_planversion_id NUMBER(10);
    t_time_template_id NUMBER(10);
    t_to_level  NUMBER(2);
    t_year_level  NUMBER(2):=47; --the TOP parent_level--
    t_num_periods NUMBER(3);
    t_dir_period_flg NUMBER(1);
    t_inc_curr_period_flg NUMBER(1);
    t_cutoff_date DATE;
    t_start_date DATE;
    t_end_date DATE;
    t_event_id NUMBER(10);

    t_proc_name VARCHAR2(30):='p_upd_dynamic_dates';
    t_call VARCHAR2(255);
    n_sqlnum NUMBER(6);
    v_sql VARCHAR2(16000);
    t_error_level VARCHAR2(6):= 'info';
    t_error_msg	 VARCHAR2(1000) := NULL;
BEGIN
n_sqlnum:=1000;
t_call := t_proc_name || ' ( ' ||
    in_planworksheet_id ||
    ' ) ';
maxdata.p_log (t_proc_name, t_error_level, t_call, 'START', n_sqlnum);

n_sqlnum:=2000;
SELECT time_template_id,planversion_id INTO t_time_template_id,t_planversion_id
FROM maxdata.planworksheet
WHERE planworksheet_id=in_planworksheet_id;

IF(t_time_template_id IS NULL)
OR(t_planversion_id!=-3) THEN --if not a PA worksheet
    RETURN;
END IF;

n_sqlnum:=2100;
SELECT to_level,num_periods,dir_period_flg,inc_curr_period_flg
INTO t_to_level,t_num_periods,t_dir_period_flg,t_inc_curr_period_flg
FROM maxdata.dimset_template
WHERE template_id=t_time_template_id
  AND path_id=50;

IF(COALESCE(t_num_periods,0)=0)THEN --not a dynamic time template--
    RETURN;
END IF;

IF(t_to_level=47)THEN
    t_year_level:=-1; --special case; year-levels have no parent--
END IF;

n_sqlnum:=2200;
--find the start_date of the current period--
SELECT start_date INTO t_cutoff_date
FROM maxdata.v_time_level
WHERE time_level=t_to_level AND parent_level=t_year_level
  AND TRUNC(SYSDATE) BETWEEN start_date AND end_date;

n_sqlnum:=2300;
--if current period is not included:
--  calculate the offset,either -1 or +1 from the current period--
IF(t_inc_curr_period_flg=0)THEN
    IF(t_dir_period_flg=2)THEN
        SELECT MAX(start_date) INTO t_cutoff_date
        FROM maxdata.v_time_level
        WHERE time_level=t_to_level AND parent_level=t_year_level
          AND start_date<t_cutoff_date;
    ELSE
        SELECT MIN(start_date) INTO t_cutoff_date
        FROM maxdata.v_time_level
        WHERE time_level=t_to_level AND parent_level=t_year_level
          AND start_date>t_cutoff_date;
    END IF;
END IF;

n_sqlnum:=2400;
--lookup the start_date and end_date of the dynamic period--
IF(t_dir_period_flg=2)THEN --backwards to the past
    SELECT MIN(START_DATE),MAX(END_DATE) INTO t_start_date,t_end_date
    FROM (SELECT start_date,end_date FROM maxdata.v_time_level
           WHERE time_level=t_to_level
             AND parent_level=t_year_level
             AND start_date<=t_cutoff_date
           ORDER BY start_date DESC)
    WHERE ROWNUM<=t_num_periods;
ELSE --forwards to the future
    SELECT MIN(START_DATE),MAX(END_DATE)  INTO t_start_date,t_end_date
    FROM (SELECT start_date,end_date FROM maxdata.v_time_level
           WHERE time_level=t_to_level
             AND parent_level=t_year_level
             AND start_date>=t_cutoff_date
           ORDER BY start_date)
    WHERE ROWNUM<=t_num_periods;
END IF;

n_sqlnum:=3000;
--Finally, update using the new dates--
UPDATE maxdata.dimset_template
   SET start_date=t_start_date,
       end_date=t_end_date
WHERE template_id=t_time_template_id;

COMMIT;

--------------------------------------------------------------------------------
--NOTE: Event(level=45) MUST already have been added by the app--
--Delete other levels if any exist--
n_sqlnum:=5000;
DELETE FROM maxdata.dimset_template_lev
 WHERE template_id=t_time_template_id
   AND level_number>45;

--Always add year level--
n_sqlnum:=5100;
INSERT INTO maxdata.dimset_template_lev
    (template_id,level_number,level_seq,level_incl_flag,partial_flag,level_name)
VALUES(t_time_template_id,47,1,1,1,'Year');

--Add other levels as specified in path_seg table--
n_sqlnum:=5200;
INSERT INTO maxdata.dimset_template_lev
    (template_id,level_number,level_seq,level_incl_flag,partial_flag,level_name)
SELECT t_time_template_id,lowerlevel_id,ROWNUM+1,1,1,
  (CASE lowerlevel_id WHEN 48 THEN 'Season' WHEN 49 THEN 'Quarter' WHEN 50 THEN 'Month' WHEN 51 THEN 'Week' END)
 FROM path_seg
WHERE path_id=50
ORDER BY lowerlevel_id;

COMMIT;

--NOTE: Event member MUST already have been added by the app; otherwise error--
n_sqlnum:=6000;
SELECT member_id INTO t_event_id
  FROM maxdata.dimset_template_mem
 WHERE template_id=t_time_template_id
   AND level_number=45;

--Delete other members if any exist--
n_sqlnum:=6100;
DELETE FROM maxdata.dimset_template_mem
 WHERE template_id=t_time_template_id
   AND level_number>45;

--Add bottom-level members--
n_sqlnum:=6200;
INSERT INTO maxdata.dimset_template_mem
(template_id,level_number,member_id,member_name,parent_member_id,exclude_flag,partial_flag,removed_flag,days_in_period1,visible_flg)
SELECT t_time_template_id,time_level,time_id,time_name||' '||cycle_id,-1,0,0,0,days_in_period,1
    FROM (SELECT * FROM maxdata.v_time_level
           WHERE time_level=t_to_level AND parent_level=t_year_level
             AND start_date>=t_start_date AND end_date<=t_end_date
           ORDER BY start_date DESC)
    WHERE ROWNUM<=t_num_periods;

--Add upper-level members--
n_sqlnum:=6300;
INSERT INTO maxdata.dimset_template_mem
(template_id,level_number,member_id,exclude_flag,partial_flag,removed_flag,visible_flg)
SELECT DISTINCT t_time_template_id,parent_level,parent_id,0,0,0,1
FROM maxdata.v_time_level
WHERE parent_level>0
  AND time_level=t_to_level
  AND time_id IN
     (SELECT member_id FROM maxdata.dimset_template_mem WHERE template_id=t_time_template_id);

--Lookup and assign parent_member_id, member_name--
n_sqlnum:=7000;
DECLARE
    CURSOR c_parent_level IS
    SELECT * FROM maxdata.path_seg
     WHERE path_id=50
       AND lowerlevel_id<=t_to_level --include bottom level
     ORDER BY lowerlevel_id DESC;
BEGIN
FOR r_lev IN c_parent_level LOOP
    DECLARE
        CURSOR c_upper_mem IS
        SELECT VT.*
          FROM maxdata.v_time_level VT
          JOIN maxdata.dimset_template_mem TM
            ON(    VT.time_level=TM.level_number
               AND VT.time_id=TM.member_id
               AND VT.parent_level=r_lev.higherlevel_id
               AND TM.level_number=r_lev.lowerlevel_id
               AND TM.template_id=t_time_template_id);
    BEGIN
    FOR r_mem in c_upper_mem  LOOP
       UPDATE maxdata.dimset_template_mem
          SET days_in_period1=r_mem.days_in_period, --upper-level will be recalculated in next step--
              parent_member_id=r_mem.parent_id,
              member_name=r_mem.time_name||' '||r_mem.cycle_id
        WHERE template_id=t_time_template_id
          AND level_number=r_mem.time_level AND member_id=r_mem.time_id;
    END LOOP;
    END;
--COMMIT;
END LOOP;
END;


--Calculate days_in_period1(which is a sum of its children) for upper-levels only--
n_sqlnum:=8000;
DECLARE
    CURSOR c_parent_level IS
    SELECT * FROM maxdata.path_seg
     WHERE path_id=50
       AND higherlevel_id<t_to_level --exclude bottom level
     ORDER BY higherlevel_id DESC;
BEGIN
FOR r_lev IN c_parent_level LOOP
    DECLARE
        CURSOR c_upper_mem IS
        SELECT member_id
          FROM maxdata.dimset_template_mem
         WHERE template_id=t_time_template_id
           AND level_number=r_lev.higherlevel_id;
    BEGIN
    FOR r_mem in c_upper_mem  LOOP
       UPDATE maxdata.dimset_template_mem
          SET days_in_period1=
                (SELECT SUM(days_in_period1)
                 FROM maxdata.dimset_template_mem
                 WHERE template_id=t_time_template_id
                   AND level_number=r_lev.lowerlevel_id
                   AND parent_member_id=r_mem.member_id)
       WHERE template_id=t_time_template_id
         AND level_number=r_lev.higherlevel_id AND member_id=r_mem.member_id;
    END LOOP;
    END;
END LOOP;
--COMMIT;
END;



--Year and Event members need special treatment--
n_sqlnum:=9100;
UPDATE maxdata.dimset_template_mem
   SET parent_member_id=t_event_id
  WHERE template_id=t_time_template_id
    AND level_number=47;

n_sqlnum:=9200;
UPDATE maxdata.dimset_template_mem TM
   SET member_name=(SELECT time_name||' '||cycle_id
                    FROM maxdata.v_time_level
                    WHERE time_level=47 AND time_id=TM.member_id)
  WHERE template_id=t_time_template_id
    AND level_number=47;

n_sqlnum:=9300;
UPDATE maxdata.dimset_template_mem
   SET days_in_period1=(SELECT SUM(days_in_period1)
                    FROM maxdata.dimset_template_mem
                    WHERE template_id=t_time_template_id AND level_number=47)
  WHERE template_id=t_time_template_id
    AND level_number=45;


COMMIT;


EXCEPTION
WHEN OTHERS THEN
	t_error_level:='error';
    t_error_msg := SQLERRM || ' (' || t_call ||', SQL#:' || n_sqlnum || ')';
	ROLLBACK;
	maxdata.p_log (t_proc_name, t_error_level, t_error_msg, v_sql, n_sqlnum);
	RAISE_APPLICATION_ERROR(-20001,t_error_msg);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_DYNAMIC_DATES" TO "MADMAX";
