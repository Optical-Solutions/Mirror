--------------------------------------------------------
--  DDL for Procedure P_LOG_MODEL_RES_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_LOG_MODEL_RES_STATUS" 
as

iSql 		long;
cnt 		number;

begin

select nvl(max(log_id),0) into cnt from maxdata.model_res_status_log;

insert into maxdata.model_res_status_log(log_id,
    MERCH_LEVEL,
    LEVEL_NAME,
    LAST_UPDATE,
    MODEL_USERID,
    MODEL_NAME,
    LIVE_USERID,
    LIVE_NAME,
    RES_STATUS )
select cnt+1,
    MERCH_LEVEL,
    LEVEL_NAME,
    LAST_UPDATE,
    MODEL_USERID,
    MODEL_NAME,
    LIVE_USERID,
    LIVE_NAME,
    RES_STATUS  from maxdata.model_res_status;

isql:='truncate table maxdata.model_res_status';
execute immediate isql;

insert into maxdata.model_res_status (MERCH_LEVEL,LEVEL_NAME,LAST_UPDATE,MODEL_USERID,MODEL_NAME ,LIVE_USERID,LIVE_NAME,RES_STATUS)
select a.merch_level,b.level_name, sysdate,a.model_userid,a.model_name,a.live_item_userid,a.live_name,to_number(a.comment_1)
from maxdata.lvxref_model_res a, maxdata.hier_level b where b.hier_id=11 and b.level_id=(a.merch_level+10);

commit;


end;

/
