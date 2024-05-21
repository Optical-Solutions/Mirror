--------------------------------------------------------
--  DDL for Procedure INS_CORP_STORE_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."INS_CORP_STORE_LOG" 
(in_log_id     varchar2,
 in_log_level  varchar2,
 in_log_text   varchar2,
 in_log_text2  varchar2,
 in_log_nbr    number,
 in_log_nbr2   number) as
BEGIN
insert into maxdata.corp_store_log
        (log_id,
         log_level,
         log_text,
         log_text2,
         log_nbr,
         log_nbr2,
         log_date
        )
        values
        (in_log_id,
         in_log_level,
         in_log_text,
         in_log_text2,
         in_log_nbr,
         in_log_nbr2,
         sysdate
         );
END;

/

  GRANT EXECUTE ON "MAXDATA"."INS_CORP_STORE_LOG" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."INS_CORP_STORE_LOG" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."INS_CORP_STORE_LOG" TO "MAXUSER";
