--------------------------------------------------------
--  DDL for Procedure P_DROP_T_CL_TBL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_T_CL_TBL" 
AS
/*------------------------------------------------------------------------
$Log: 2206_p_drop_t_cl_tbl.sql,v $
Revision 1.9.14.1  2009/07/07 19:57:35  anchan
FIXID S0550851: use 'marketmax.clusterhistory.valid.days'

Revision 1.9  2007/06/19 14:39:20  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.5  2006/02/17 22:18:57  healja
Replace $id with $Log

Revision 1.4  2006/02/17 21:43:14  makirk
Changed cursor to check for MAXTEMP tables, changed drop proc to maxtemp.p_exec_temp_ddl, removed ID from comments

Revision 1.3  2005/08/03 17:16:04  dirapa
No comment given.

Revision 1.2  2005/08/03 17:12:11  dirapa
--V6.1
--6.1.0-001 07/29/05 Diwakar    Changed curosr select to fetch only cluster table names


-- Change History:

-- V5.3.4
-- Backported from 5.4
-- V5.4
-- 5.4.0-015 09/16/2002 Sachin Ghaisas  Added code to handle timeshift dataversion
-- 5.4.0-000 08/14/2002 Sachin Ghaisas  Initial entry.

-- Description:
-- Drop the t_cl temporary tables if there are no enteries
-- in maxdata.cl_hist_status table or
-- if the entry is older than the CL_HIST_VALID_DAYS
------------------------------------------------------------------------*/

t_valid_days       NUMBER;
t_row_count        NUMBER;
t_last_accessed    DATE;
t_drop_table       NUMBER;
v_sql              VARCHAR2(1000);
n_sqlnum           NUMBER;
t_dv_time_id       NUMBER;
t_future_param_int NUMBER(10,0)    := -1;
t_future_param_var VARCHAR2(10)    := NULL;
t_ignore_error     NUMBER          := 0;   --0 is Raise exception. 1 to ignore when called p_execute_ddl_sql

BEGIN

n_sqlnum := 1000;

SELECT COALESCE(property_value,default_value) INTO t_valid_days
FROM maxdata.t_application_property
WHERE property_key='marketmax.clusterhistory.valid.days';

IF(t_valid_days<=0)THEN
    RETURN;
END IF;

DECLARE CURSOR c_get_t_cl_tbl
IS
SELECT owner || '.' || table_name table_name
FROM all_tables
WHERE owner = 'MAXTEMP'
AND table_name LIKE 'T\_CL\_%\_%' ESCAPE '\';

BEGIN
FOR x IN c_get_t_cl_tbl
LOOP
    t_drop_table := 0;

    t_row_count := 0;

    n_sqlnum := 2000;

    BEGIN
        SELECT 1,
               last_accessed
        INTO   t_row_count,
               t_last_accessed
        FROM maxdata.cl_hist_status
        WHERE  UPPER(table_nm) = x.table_name;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                t_row_count := 0;
    END;


    IF t_row_count = 1 THEN
    BEGIN
        n_sqlnum := 3000;

        IF  TO_CHAR(SYSDATE,'J')- TO_CHAR(t_last_accessed,'J') > t_valid_days THEN
            t_drop_table := 1;
        END IF;
    END;
    ELSE
    BEGIN
        t_drop_table := 1;
    END;
    END IF;

    IF t_drop_table = 1 THEN
    BEGIN
        n_sqlnum := 4000;

        v_sql := 'DROP TABLE '|| x.table_name;

        maxtemp.p_exec_temp_ddl(t_ignore_error, v_sql, NULL, NULL, NULL, NULL, t_future_param_int, t_future_param_int, t_future_param_var);

        IF t_row_count > 0 THEN
        BEGIN
            n_sqlnum := 5000;

            DELETE maxdata.cl_hist_status
            WHERE UPPER(table_nm) = x.table_name;
        END;
        END IF;
    END;
    END IF;
END LOOP;
COMMIT;
END;
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_T_CL_TBL" TO "MADMAX";
