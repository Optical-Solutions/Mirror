--------------------------------------------------------
--  DDL for Procedure BP_JOB_PURGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."BP_JOB_PURGE" 
---
---  Removing the jobs older thant 14 days
---         
as
    iJobId_tbl    varchar2(30);
    iSql    long;
    iCtr    int;
begin

iJobId_tbl:='t_mcx_bp_job';

select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper(iJobId_tbl);
            
if iCtr = 0 then
    iSql := 'create table maxdata.'||iJobId_tbl||'(job_queue_id number(10))  nologging pctfree 0';
    execute immediate iSql;
else
    isql:='truncate table  maxdata.'||iJobId_tbl;
     execute immediate iSql;
end if;

iSql:='insert into maxdata.'||iJobId_tbl||'(job_queue_id) '||
    ' select job_queue_id from maxdata.bpjq_job_queue where job_type_cd in (7,13,24)'||
    ' and to_number(trunc(sysdate)-trunc(actual_finish_dttm))> 14 and job_status_cd=2';
execute immediate iSql;

iSql:='insert into maxdata.'||iJobId_tbl||'(job_queue_id) '||
    ' select job_queue_id from maxdata.bpjq_job_queue where job_type_cd in (7,13,24)'||
    ' and to_number(trunc(sysdate)-trunc(actual_start_dttm))> 14 and job_status_cd=3';
execute immediate iSql;

iSql:='insert into maxdata.'||iJobId_tbl||'(job_queue_id) '||
    ' select job_queue_id from maxdata.bpjq_job_queue where '||
    ' to_number(trunc(sysdate)-trunc(scheduled_start_dttm))> 28 and job_status_cd=0';
execute immediate iSql;
commit;

iSql:='insert into maxdata.'||iJobId_tbl||'(job_queue_id) '||
    ' select distinct child_queue_id from maxdata.bpjd_job_dependency a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    ' where a.parent_queue_id=b.job_queue_id)';
execute immediate iSql;

iSql:='insert into maxdata.'||iJobId_tbl||'(job_queue_id) '||
    ' select distinct parent_queue_id from maxdata.bpjd_job_dependency a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    ' where a.child_queue_id=b.job_queue_id)';
execute immediate iSql;

commit;

--- remove the old jobs

iSql:='delete from maxdata.bpjd_job_dependency a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    '  where  a.parent_queue_id=b.job_queue_id)';
execute immediate iSql;

iSql:='delete from maxdata.bpjd_job_dependency a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    '  where a.child_queue_id=b.job_queue_id)';
execute immediate iSql;

iSql:='delete from maxdata.bpjp_job_parameter a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    '  where a.job_queue_id=b.job_queue_id)';
execute immediate iSql;

iSql:='delete from maxdata.bpjq_job_queue a where exists (select 1 from maxdata.'||iJobId_tbl||' b'||
    '  where a.job_queue_id=b.job_queue_id)';
execute immediate iSql;

commit;
    
end;

/
