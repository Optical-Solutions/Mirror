--------------------------------------------------------
--  DDL for Procedure PROC_SAS_RECLASS_JOB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."PROC_SAS_RECLASS_JOB" (p_cmd out varchar2 )is


v_row_id rowid;
v_job_row sas_reclass_job_control%rowtype;

v_step_id     sas_reclass_job_control.step_id%type;
v_sub_step_id sas_reclass_job_control.sub_step_id%type;

begin
/*
export SQL_PLUS=/app/oracle/product/11.1_old/bin/sqlplus
export SQL_DIR=/app/mdi/SAS/retail/plan_61/MDI/noncore/scripts/reclass_auto/sql
export SQL_USER=eric/ericdata@hqaix07.usmc-mccs.org/sastst
$SQL_PLUS -S $SQL_USER @$SQL_DIR/test.sql
sas_reclass_job.sql
*/

for rec in (select rowid row_id, sr.* from SAS_RECLASS_JOB_CONTROL sr
            where reclass_lvl = 'MAIN'
            order by step_id, Sub_Step_Id)
loop


if rec.step_status = 'START'
then 
  if rec.verify_prev_step_complete = 1  
  then
    select * into v_job_row from SAS_RECLASS_JOB_CONTROL where rowid = v_row_id;
    if v_job_row.step_status = 'COMPLETE'
    then  
      p_cmd := rec.program_dir||rec.program_name;
      return;
   else 
      update SAS_RECLASS_JOB_CONTROL
      set step_status = 'ERROR',
      return_code = '5507 Previous job not completed'
      where rowid = rec.row_id;
   end if;
 else
  p_cmd := rec.program_dir||rec.program_name;
  return;
 end if;
end if;

v_row_id := rec.row_id ;

end loop;

end proc_sas_reclass_job;


/
