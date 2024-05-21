--------------------------------------------------------
--  DDL for Procedure P_MCX_SUBMIT_CUMS_BATCH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_MCX_SUBMIT_CUMS_BATCH" 
as
-- date: 10-31-2012 ec added an update to reset bacth mode=0
iPlanVersion_id 	int;
iPlanMaster_id 		int;
iSeqNum 		int;
iTimeId 		int;
ipName 			varchar2(30);
imName 			varchar2(30);

iCtr			int;

begin

	
	select lv1time_lkup_id into iTimeId from maxapp.lv1time where cycle_id = (select current_cycle from maxapp.mmax_config);

	select planmaster_id, planversion_id, name into iPlanMaster_id, iPlanVersion_id, ipName from maxdata.planversion 
		where name like '%CUMS' and from_time_level=47 and from_time_id=iTimeId and rownum=1;
		
	select count(*) into iCtr from MAXDATA.BPJQ_JOB_QUEUE a, MAXDATA.BPJP_JOB_PARAMETER b 
		where a.JOB_QUEUE_ID=b.JOB_QUEUE_ID and b.NUMERIC_PARAMETER = iPlanVersion_id and a.job_status_cd in (0,1);
		
	
	if iCtr > 0 then return; end if;
	
	
	select name into imName from maxdata.planmaster where planmaster_id=iPlanMaster_id;

	imName:=imName||ipName;

	/* insert into job_quque */

--	select seq_num + 1 into iSeqNum from maxapp.sequence where entity_type=101 and level_type=1 for update;

--	update maxapp.sequence set seq_num=seq_num + 2 where entity_type=101 and level_type=1;
--	commit;

/*
	Insert into MAXDATA.BPJQ_JOB_QUEUE
	   (JOB_QUEUE_ID, JOB_NM, JOB_TYPE_CD, JOB_CREATE_DTTM, SCHEDULED_START_DTTM, JOB_STATUS_CD, PRIORITY_CD, PARENT_NM)
	 Values(iSeqNum, 'Submit All', 13, sysdate, sysdate, 0, 50, imName);

	Insert into MAXDATA.BPJP_JOB_PARAMETER
	   (JOB_QUEUE_ID, PARAMETER_SEQUENCE_NO, NUMERIC_PARAMETER, PARAM_NAME)
	 Values(iSeqNum, 1, iPlanVersion_id, 'PLANVERSION_ID');
	 */
  update maxdata.planversion set batch_status = null,batch_mode=0, bat_oper_flag=0 where planversion_id=iPlanVersion_id;
	update maxdata.planversion set batch_status = null,batch_mode=1, bat_oper_flag=5 where planversion_id=iPlanVersion_id;
	
	commit;

end;

/
