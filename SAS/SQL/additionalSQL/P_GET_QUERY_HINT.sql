--------------------------------------------------------
--  DDL for Procedure P_GET_QUERY_HINT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_QUERY_HINT" (
           in_QueryType      VARCHAR2,
           in_TableList      VARCHAR2,  -- Format of "in_TableList" string must be: [MAXDATA.TABLE1][MAXDATA.TABLE2]...
           out_TableHint OUT VARCHAR2,
           out_QueryHint OUT VARCHAR2
) AS

/*
------------------------------------------------------------------------
Change History
$Log: 2110_p_get_query_hint.sql,v $
Revision 1.6  2007/06/19 14:39:57  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.2  2006/04/14 16:39:26  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit


V5.6.0
 5.6.0-029-18   06/07/04   Andy Chang    script renumbered
 5.6.0-035      05/10/04   Andy Chang    #16631 Initital Entry.

Description:

This procedure retrieves CUSTOM HINTS to be used with dynamic queries.

The p_get_query_hint procedure first strips off the user-spec "MAXDATA." from the list of TableNames.
Note also that in all cases, the table placeholder "%T" will be replaced with the specified "(TableName)"
in the final output variables.

When the p_get_query_hint procedure is called:
Returns the table_hint variable:
-       checks to see if the matching row with specified QUERY_TYPE and TABLE_NAME exists;
        returns the custom_hint string if specified, otherwise returns the default_hint string.
-       If no exact match, checks to see if the QUERY_TYPE and wild-card TABLE_NAME spec exists;
        returns the custom_hint or default_hint string with tablename imbedded.
-       Otherwise, return null.

Returns the query_hint variable:

-       checks to see if the matching row with specified QUERY_TYPE of QUERY hint_type exists;
        returns the custom_hint or default_hint string.
-       Otherwise, return null.
-

--------------------------------------------------------------------------------
*/

n_sqlnum                NUMBER(10,0);
t_proc_name             VARCHAR2(32)        := 'p_get_query_hint';
t_error_level           VARCHAR2(6)         := 'info';
t_call                  VARCHAR2(1000);
v_sql                   VARCHAR2(1000)      := NULL;
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);
--
t_StrLen                NUMBER(10);
t_BegPos                NUMBER(10);
t_EndPos                NUMBER(10);
t_QueryType             VARCHAR2(30);
t_TableName             VARCHAR2(255);
t_TableList             VARCHAR2(255);

BEGIN

n_sqlnum := 1000;
/*
Since this procedure is called 1000's of times during the day, don't bother with logging...

-- Log the parameters of the procedure

t_call := t_proc_name || ' ( ' ||
        in_QueryType || ',' ||
        in_TableList  ||
        ' ) ';
maxdata.ins_import_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
COMMIT;
*/
n_sqlnum := 2000;

IF (in_QueryType IS NULL) THEN
BEGIN
        RAISE_APPLICATION_ERROR (-20001, 'Must specify the parameter "QueryType".');
END;
END IF;

IF (in_TableList IS NOT NULL) AND (in_TableList NOT LIKE '[%]') THEN
   BEGIN
        RAISE_APPLICATION_ERROR (-20001, 'Invalid "TableList" specified. Param:'||in_TableList);
   END;
END IF;

t_QueryType   := UPPER(in_QueryType);
out_QueryHint := NVL(maxdata.f_query_hint(t_QueryType),' ');
t_TableList   := REPLACE(UPPER(in_TableList),'MAXDATA.',''); --remove "MAXDATA."
t_BegPos      := 1;
t_EndPos      := INSTR(t_TableList, ']', 1,1); --locate the end of first TableName
out_TableHint := ' ';

WHILE t_EndPos > 0
LOOP
     t_StrLen      := t_EndPos-t_BegPos-1;
     t_TableName   := SUBSTR(t_TableList,t_BegPos+1,t_StrLen);
     out_TableHint := out_TableHint||' '||NVL(maxdata.f_table_hint(t_QueryType,t_TableName),' ');
     t_BegPos      := t_EndPos+1; --shift the start of search position
     t_EndPos      := INSTR(t_TableList, ']', t_BegPos,1);--locate the end of next TableName
END LOOP;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := SUBSTR(v_sql,1,255);
                        maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';

                t_sql2 := SUBSTR(v_sql,1,255);
                t_sql3 := SUBSTR(v_sql,256,255);
                maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_GET_QUERY_HINT" TO "MADMAX";
