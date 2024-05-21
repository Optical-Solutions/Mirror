--------------------------------------------------------
--  DDL for Procedure PURGE_RECYCLEBIN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."PURGE_RECYCLEBIN" AS
BEGIN
  execute immediate 'purge recyclebin';
END;

/
