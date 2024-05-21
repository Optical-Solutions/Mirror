--------------------------------------------------------
--  DDL for Procedure P_CREATE_GPF_TMPTBLS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CREATE_GPF_TMPTBLS" 
    (in_basename VARCHAR2,
    out_tbl1 out VARCHAR2,
    out_tbl2 out VARCHAR2)
AS

/* -------------------------------------------------------------------------------

$Log: 2169_p_create_gpf_tmptbls.sql,v $
Revision 1.8  2007/06/19 14:39:32  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/03/02 21:50:40  makirk
Added T_GPF_ prefix to table names

Revision 1.2  2006/01/09 19:22:59  makirk
Modified for creating temp tables under maxtemp

Revision 1.1  2005/10/04 21:53:42  joscho
Replace p_clear/create_tmp_tbl


Description:

This procedure is called by Global Product Filter (GPF) to create two temp tables.

Usage:

The app calls this procedure, so any interface change should be coordinated with the app.

----------------------------------------------------------------------------------- */

t_seq_id       NUMBER;
t_msg          VARCHAR2(255);
v_sql          VARCHAR2(255);
t_cols         VARCHAR2(255);
t_ignore_error NUMBER         := 0; -- 0 is raise exception.  1 is to ignore
t_future_int   NUMBER(2)      := -1;
t_char_null    CHAR(1)        := NULL;

BEGIN
    t_cols := '(member_id number(10,0), filterfield number(10,0), worksheet_id number(10,0))';

    maxapp.p_get_next_key(1005, 2, 1, t_seq_id, t_msg);
    IF t_msg IS NOT NULL THEN
        t_msg := 'Error from p_get_next_key: ' || t_msg;
        RAISE_APPLICATION_ERROR (-20001, t_msg);
    END IF;

    out_tbl1 := 'maxtemp.' || 'T_GPF_' || in_basename || '_'||TO_CHAR(t_seq_id) ||'_1';
    out_tbl2 := 'maxtemp.' || 'T_GPF_' || in_basename || '_'||TO_CHAR(t_seq_id) ||'_2';

    -- Drop if exists. Errors are suppressed within the drop proc.

    maxdata.p_drop_table (out_tbl1);
    maxdata.p_drop_table (out_tbl2);

    -- Create temp tables.

    v_sql := 'CREATE TABLE ' || out_tbl1 || t_cols ;
    maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);
--  execute immediate v_sql;

    t_ignore_error := 0;
    v_sql := 'GRANT ALL ON '|| out_tbl1 ||' TO maxdata, madmax, maxuser';
    maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);


    v_sql := 'CREATE TABLE ' || out_tbl2 || t_cols ;
    maxtemp.p_exec_temp_ddl(t_ignore_error,v_sql,t_char_null,t_char_null,t_char_null,t_char_null,t_future_int,t_future_int,t_char_null);
--  execute immediate v_sql;

    t_ignore_error := 0;
    v_sql := 'GRANT ALL ON '|| out_tbl2 ||' TO maxdata, madmax, maxuser';
    maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, t_char_null, t_char_null, t_char_null, t_char_null, t_future_int, t_future_int, t_char_null);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_CREATE_GPF_TMPTBLS" TO "MADMAX";
