--------------------------------------------------------
--  DDL for Procedure P_CUSTOM_AGGR
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CUSTOM_AGGR" (
    in_cube_id    IN  NUMBER,
    in_pw_id      IN  NUMBER,
    in_kpi_dv_id  IN  NUMBER,
    in_future1    IN  NUMBER,
    in_future2    IN  NUMBER,
    in_debug_flag IN  NUMBER
) as

/*------------------------------------------------------------------
-- Change History:

$Log: 2176_p_custom_aggr.sql,v $
Revision 1.6  2007/06/19 14:39:31  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.2  2006/02/15 23:29:21  joscho
Synch up the parameter lists of p_get_cl_hist and p_custom_aggr.


-- V5.3.4
-- 02/25/03   Sachin			Added parameters
-- 02/20/03   Sachin			Created.

-- Description:
-- This procedure is a dummy procedure for unique merch count feature.
--------------------------------------------------------------------*/

t_call VARCHAR2(1000);

BEGIN


-- Log the parameters of the proc.

t_call :=  ' p_custom_aggr(' ||
    COALESCE(in_cube_id,    -123) || ',' ||   -- NVL(int, 'NULL') returns error because of diff datatype.
    COALESCE(in_pw_id,      -123) || ',' ||
    COALESCE(in_kpi_dv_id,  -123) || ',' ||
    COALESCE(in_future1,    -123) || ',' ||
    COALESCE(in_future2,    -123) || ',' ||
    COALESCE(in_debug_flag, -123) || ')';
maxdata.ins_import_log ('p_custom_aggr','info', t_call, null, null, null);
COMMIT;

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_CUSTOM_AGGR" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_CUSTOM_AGGR" TO "MAXUSER";
