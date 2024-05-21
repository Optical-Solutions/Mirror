--------------------------------------------------------
--  DDL for Procedure P_MFINC_MODELSKU_VIEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_MFINC_MODELSKU_VIEW" 
--- 9/26/11	Modified to add create table statement
--- 5/4/12	Modified for MCX
AS

iSql                 VARCHAR2(2000)  := NULL;
iCtr		     number(2);

BEGIN
    maxdata.Ins_Import_Log('p_mfinc_modelsku_view','Information',
            'start merch_id temp tables populate', NULL
            ,NULL,NULL) ;
    /* commend out for MCX
    select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper('merch_ids8_with_mfinc');

    if iCtr = 0  then
	iSql := 'create table maxdata.merch_ids8_with_mfinc as select merch_id from maxdata.mfinc where 1=2';
	execute immediate iSql;
    else
    	iSql := 'truncate table maxdata.merch_ids8_with_mfinc';
    	EXECUTE IMMEDIATE iSql;
    end if;

    iSql := 'insert into maxdata.merch_ids8_with_mfinc select merch_id from maxdata.mfinc WHERE merch_level = 8 AND location_level = 1 AND time_level = 47';
    EXECUTE IMMEDIATE iSql;
    */

    select count(1) into iCtr from sys.all_tables where owner='MAXDATA' and table_name =upper('merch_ids7_with_mfinc');

    if iCtr = 0  then
	iSql := 'create table maxdata.merch_ids7_with_mfinc as select merch_id from maxdata.mfinc where 1=2';
	execute immediate iSql;
    else
    	iSql := 'truncate table maxdata.merch_ids7_with_mfinc';
    	EXECUTE IMMEDIATE iSql;
    end if;

    iSql := 'insert into maxdata.merch_ids7_with_mfinc select merch_id from maxdata.mfinc WHERE merch_level = 7 AND location_level = 1 AND time_level = 47';
    EXECUTE IMMEDIATE iSql;
    /* /* commend out for MCX
    if iCtr = 0  then
	iSql := 'create table maxdata.merch_ids6_with_mfinc as select merch_id from maxdata.mfinc where 1=2';
	execute immediate iSql;
    else
    	iSql := 'truncate table maxdata.merch_ids6_with_mfinc';
    	EXECUTE IMMEDIATE iSql;
    end if;

    iSql := 'insert into maxdata.merch_ids6_with_mfinc select merch_id from maxdata.mfinc WHERE merch_level = 6 AND location_level = 1 AND time_level = 47';
    EXECUTE IMMEDIATE iSql;
    */

    maxdata.Ins_Import_Log('p_mfinc_modelsku_view','Information',
            'end merch_id temp tables populate', NULL
            ,NULL,NULL) ;
END;

/
