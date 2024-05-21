--------------------------------------------------------
--  DDL for Procedure P_DROP_TABLE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_TABLE" (
        in_table_name VARCHAR2
) AS
/*----------------------------------------------------------------
$Log: 2168_p_drop_table.sql,v $
Revision 1.7  2007/06/19 14:39:33  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/11/22 17:02:23  makirk
Defect S0387699

Only drops tables in MAXTEMP

Revision 1.2  2005/10/04 21:51:44  joscho
Clean up and add error handler in order to replace 6.1 GPF  p_drop_tmp_tbl


Usage:

This procedure is used by the app to drop a table.
-----------------------------------------------------------------*/

v_sql VARCHAR2(1000);

BEGIN

-- Make sure only the maxtemp schema is being addressed
IF UPPER(SUBSTR(in_table_name,1,8)) = 'MAXTEMP.' THEN
        v_sql := 'DROP TABLE ' || in_table_name;

        -- Ignore any error during drop.
        BEGIN
                EXECUTE IMMEDIATE v_sql;

                EXCEPTION
                        WHEN OTHERS THEN
                                NULL;
        END;
ELSE
        v_sql := 'Invalid table for drop: '||in_table_name;
        RAISE_APPLICATION_ERROR (-20001,v_sql);
END IF;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_TABLE" TO "MADMAX";
