--------------------------------------------------------
--  DDL for Procedure P_LOG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_LOG" (
    in_log_id       VARCHAR2,
    in_log_level    VARCHAR2,
    in_long_string  VARCHAR2,
    in_short_string VARCHAR2,
    in_sqlnum       NUMBER) AS

/* ----------------------------------------------------------------------
$Log: 2051_p_log.sql,v $
Revision 1.8  2007/06/19 14:40:04  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.4  2006/11/29 15:12:35  joscho
Removed AUTONOMOUS TRANSACTION because underlying procedure (ins_import_log) has it.

Revision 1.3  2006/09/27 19:51:30  makirk
Added a check on short_string to make sure it doesn't overflow

Revision 1.1  2006/09/20 20:43:49  makirk
Renaming as per JC/RG
Added for rename from 2051_p_ins_long_import_log.sql.
See originally named file for history prior to the rename.

Revision 1.6  2006/09/20 20:42:44  makirk
Changed parameter names

Revision 1.5  2006/09/14 16:14:02  joscho
Handle NULL string

Revision 1.4  2006/05/22 15:42:14  makirk
Changed commit to be an automomous transaction

Revision 1.3  2006/04/26 17:26:31  makirk
Removed param logging and exception block (will be done by generic error handler)

Revision 1.2  2006/04/26 17:20:56  makirk
Switched t_char_null and t_sql_seg positions

Revision 1.1  2006/03/02 20:56:07  vejang
From 2103_p_ins_long_import_log.sql
To    2051_p_ins_long_import_log.sql
Added for rename from 2103_p_ins_long_import_log.sql.
See originally named file for history prior to the rename.

Revision 1.2  2006/01/10 21:24:50  makirk
Removed in_t_call param and t_call logging in main body

Revision 1.1  2006/01/09 19:20:44  makirk
Version of p_import_log that handles long sql statements


Parameters:

    in_log_id:       maxdata.ins_import_log param log_id
    in_log_level:    maxdata.ins_import_log param log_level
    in_long_string:  maxdata.ins_import_log param v_sql
    in_short_string: maxdata.ins_import_log param v_sql2
    in_sqlnum:       maxdata.ins_import_log param n_sqlnum

------------------------------------------------------------------------
*/

t_steps         NUMBER(3);        -- The number of segments t_log_size long
t_log_size      NUMBER(3) := 255; -- The size of the "LOG_TEXT" field in import_log
t_sql_seg       VARCHAR2(255);    -- The size here must be manually changed to match t_log_size
t_short_string  VARCHAR2(255);
t_index         NUMBER(3);        -- Loop counter

n_sqlnum        NUMBER(10,0)    := 0;
t_char_null     CHAR(1)         := NULL;

BEGIN

n_sqlnum := 100;
-- Make sure the string being passed fits into the short string field
t_short_string := SUBSTR(in_short_string,1,255);

-- If the string is null, log the null and return.
n_sqlnum := 500;
IF in_long_string IS NULL THEN
    maxdata.ins_import_log (in_log_id, in_log_level, in_long_string, t_short_string, in_sqlnum, NULL);
    RETURN;
END IF;


n_sqlnum := 1000;
-- Break up string into t_log_size chunks and load them into the import_log table
t_steps  := FLOOR(LENGTH(in_long_string)/t_log_size);

n_sqlnum := 2000;
FOR t_index IN 0..t_steps LOOP
    -- It is ok to SUBSTR beyond the last character in UDB and SS
    t_sql_seg := SUBSTR(in_long_string,(t_index*t_log_size)+1,t_log_size);

    -- Put index value (+1 to start at 1) in the "future_int" field so that the original order of the sql segments can be recalled
    maxdata.ins_import_log (in_log_id, in_log_level, t_sql_seg, t_short_string, in_sqlnum, t_index+1);
END LOOP;

END p_log;

/

  GRANT EXECUTE ON "MAXDATA"."P_LOG" TO "DATAMGR";
  GRANT EXECUTE ON "MAXDATA"."P_LOG" TO "MAXTEMP";
  GRANT EXECUTE ON "MAXDATA"."P_LOG" TO "MAXAPP";
