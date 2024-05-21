--------------------------------------------------------
--  DDL for Procedure INSERT_DEL_LOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."INSERT_DEL_LOC" 
(p_level_number IN NUMBER, p_lvx_locid IN NUMBER, c_level_number IN NUMBER, c_lvx_locid IN NUMBER)
AS

BEGIN
        INSERT INTO MAXDATA.DEL_LOC(P_LEVEL_NUMBER, P_LVX_LOCID, C_LEVEL_NUMBER, C_LVX_LOCID)
            VALUES (p_level_number, p_lvx_locid, c_level_number, c_lvx_locid) ;
END;

/

  GRANT EXECUTE ON "MAXDATA"."INSERT_DEL_LOC" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."INSERT_DEL_LOC" TO "MAXUSER";
