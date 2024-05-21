--------------------------------------------------------
--  DDL for Procedure P_UPDATE_RESOLVED_MEMBER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPDATE_RESOLVED_MEMBER" 
-- 9/26/01  modified to use loadpref for the reskey variables
as
iSql         long;
X            number(2);
iCol        varchar2(20);
iPending    varchar2(20);
userid        varchar2(50);
cnt         number;
iTab        varchar2(30);
cursor res_log is
    select merch_level,live_userid from maxdata.model_res_status where res_status='3' order by merch_level asc;
Begin

--iCol:='num_user6';
--iPending:='char_user12';

For c1 in res_log loop

    select value_1 into iCol from datamgr.Loadpref where upper(key_1)='MODEL_RESKEY' and hier_level=c1.merch_level;
    select value_1 into iPending from datamgr.Loadpref where upper(key_1)='MODEL_RES_STATUS' and hier_level=c1.merch_level;

    X:=c1.merch_level;
    userid:=c1.live_userid;

    -- clear reskey --
    iSql:='update maxdata.lv'||X||'cmast set '||iCol||' = null where'||
    ' lv'||X||'cmast_userid = '''||userid||'''';

    execute immediate iSql;

    -- set pending status to resolved  --
    iSql:='update maxdata.lv'||X||'cmast set '||iPending||' = ''Resolved'' where'||
    ' lv'||X||'cmast_userid = '''||userid||'''';

    execute immediate iSql;

    commit;

end loop;

End;

/
