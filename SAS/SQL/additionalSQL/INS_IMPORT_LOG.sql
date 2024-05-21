--------------------------------------------------------
--  DDL for Procedure INS_IMPORT_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."INS_IMPORT_LOG" (
        in_log_id       VARCHAR2,
        in_log_level    VARCHAR2,
        in_log_text     VARCHAR2,
        in_log_text2    VARCHAR2,
        in_log_nbr      NUMBER,
        in_log_nbr2     NUMBER
)
AS

/*
$Id: 2050_ins_import_log.sql,v 1.7.8.1 2008/12/09 16:20:38 saghai Exp $
---------------------------------------------------------------------------------------
Change History
$Log: 2050_ins_import_log.sql,v $
Revision 1.7.8.1  2008/12/09 16:20:38  saghai
S0551550  Insert only first 255 characters in the log_text columns

Revision 1.7  2007/06/19 14:40:06  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/09/21 16:54:25  makirk
Moved import_log cleanup from ins_import_log to p_wl_cleanup_job

Revision 1.2  2006/04/14 16:39:25  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit


6.1.0-000 07/20/05   Andy  Modified so that the import_log table is pruned periodically.

Description:
  This is the ubiquitous procedure used to log parameters and/or error messages from other calling
  procedures.  It is no longer necessary to delete rows from this table outside of this proceudre.
  Since this is an AUTONOMOUS_TRANSACTION, it is not necessary to issue a COMMIT after calling this
  procedure.

  NOTE: The LOG_DATE column should be changed to TIMESTAMP type.
 ---------------------------------------------------------------------------------------
*/

PRAGMA AUTONOMOUS_TRANSACTION;

BEGIN

INSERT INTO maxdata.import_log
        (log_id,
         log_level,
         log_text,
         log_text2,
         log_nbr,
         log_nbr2,
                 log_date /* Datatype should be "TIMESTAMP" */
        )
        VALUES
        (in_log_id,
         in_log_level,
         SUBSTR(in_log_text,1,255),
         SUBSTR(in_log_text2,1,255),
         in_log_nbr,
         in_log_nbr2,
         SYSTIMESTAMP
         );

COMMIT; -- Necessary for AUTONOMOUS_TRANSACTION

END;

/

  GRANT EXECUTE ON "MAXDATA"."INS_IMPORT_LOG" TO "MAXTEMP";
  GRANT EXECUTE ON "MAXDATA"."INS_IMPORT_LOG" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."INS_IMPORT_LOG" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."INS_IMPORT_LOG" TO "MAXUSER";
