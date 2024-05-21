--------------------------------------------------------
--  DDL for Procedure P_DROP_ALL_TMP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_DROP_ALL_TMP" 
AS
/*------------------------------------------------------------------------
$Log: 2212_p_drop_all_tmp.sql,v $
Revision 1.8  2007/06/19 14:39:18  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.4  2006/02/17 22:18:59  healja
Replace $id with $Log
 2212_p_drop_all_tmp.sql,v 1.3 2005/07/29 20:47:25 dirapa Exp $

-- Change History:
-- V5.3.4
-- Backported from V5.4
-- V5.4
-- 5.4.0-027 10/24/2002 Sachin Ghaisas	Changed name from tmpl to tmp
-- 5.4.0-015 09/16/2002 Sachin Ghaisas	Initial entry.

-- Description:
-- Call the the procedures to drop the temporary tables
------------------------------------------------------------------------*/

BEGIN

	maxdata.p_drop_t_cl_tbl();
	maxdata.p_drop_t_tbl();
	maxdata.p_drop_t_tmpl_tbl();

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_DROP_ALL_TMP" TO "MADMAX";
