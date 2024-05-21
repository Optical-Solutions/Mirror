--------------------------------------------------------
--  DDL for Procedure GET_CTREE_FROM_CMAST_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."GET_CTREE_FROM_CMAST_ID" 
(	local_lv1cmast_id IN 	NUMBER,
	local_lv2cmast_id IN 	NUMBER,
	local_lv3cmast_id IN 	NUMBER,
	local_lv4cmast_id IN 	NUMBER,
	local_lv5cmast_id IN 	NUMBER,
	local_lv6cmast_id IN 	NUMBER,
	local_lv7cmast_id IN 	NUMBER,
	local_lv8cmast_id IN 	NUMBER,
	local_lv9cmast_id IN 	NUMBER,
	local_lv2ctree_id OUT 	NUMBER,
	local_lv3ctree_id OUT 	NUMBER,
	local_lv4ctree_id OUT 	NUMBER,
 	local_lv5ctree_id OUT 	NUMBER,
 	local_lv6ctree_id OUT 	NUMBER,
 	local_lv7ctree_id OUT 	NUMBER,
 	local_lv8ctree_id OUT 	NUMBER,
 	local_lv9ctree_id OUT 	NUMBER
)

AS

--Change History

--$Log: 2138_get_ctree_from_cmast_id.sql,v $
--Revision 1.6  2007/06/19 14:39:46  clapper
--FIXID AUTOPUSH: SOS 1238247
--
--Revision 1.2  2006/11/29 20:46:46  saghai
--S0391677 Added error handling
--
--


-- Created during CMAST changes
-- This procedure fetches all the CTREE ids based on the CMAST ids


n_sqlnum        NUMBER(10,0);
t_proc_name     VARCHAR2(32)    := 'get_ctree_from_cmast_id';
t_error_level   VARCHAR2(6)     := 'info';
t_call          VARCHAR2(1000);
v_sql           VARCHAR2(1000)  := NULL;
t_sql2          VARCHAR2(255);

BEGIN

n_sqlnum := 1000;

-- Log the parameters of the procedure

t_call := t_proc_name           || ' ( ' ||
        COALESCE(local_lv1cmast_id, -123)     || ',' ||
        COALESCE(local_lv2cmast_id, -123)     || ',' ||
        COALESCE(local_lv3cmast_id, -123)     || ',' ||
        COALESCE(local_lv4cmast_id, -123)     || ',' ||
        COALESCE(local_lv5cmast_id, -123)     || ',' ||
        COALESCE(local_lv6cmast_id, -123)     || ',' ||
        COALESCE(local_lv7cmast_id, -123)     || ',' ||
        COALESCE(local_lv8cmast_id, -123)     || ',' ||
        COALESCE(local_lv9cmast_id, -123)     || ',' ||
        'OUT local_lv2ctree_id to local_lv9ctree_id' ||
        ' ) ';

maxdata.p_log (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum);
--COMMIT;


IF     	local_lv9cmast_id IS NOT NULL AND
	local_lv8cmast_id IS NOT NULL AND
	local_lv7cmast_id IS NOT NULL AND
	local_lv6cmast_id IS NOT NULL AND
	local_lv5cmast_id IS NOT NULL AND
	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
        local_lv1cmast_id IS NOT NULL 	THEN

	n_sqlnum := 2000;

	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id,lv5ctree_id,lv6ctree_id,lv7ctree_id,lv8ctree_id,lv9ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id,
	   	local_lv7ctree_id,local_lv8ctree_id,local_lv9ctree_id
	FROM   	maxdata.lv9ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id AND
		lv5cmast_id=local_lv5cmast_id AND
		lv6cmast_id=local_lv6cmast_id AND
		lv7cmast_id=local_lv7cmast_id AND
		lv8cmast_id=local_lv8cmast_id AND
		lv9cmast_id=local_lv9cmast_id ;

ELSIF  	local_lv8cmast_id IS NOT NULL AND
        local_lv7cmast_id IS NOT NULL AND
	local_lv6cmast_id IS NOT NULL AND
	local_lv5cmast_id IS NOT NULL AND
	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 3000;

	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id,lv5ctree_id,lv6ctree_id,lv7ctree_id,lv8ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id,
	   	local_lv7ctree_id,local_lv8ctree_id
	FROM   	maxdata.lv8ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id AND
		lv5cmast_id=local_lv5cmast_id AND
		lv6cmast_id=local_lv6cmast_id AND
		lv7cmast_id=local_lv7cmast_id AND
		lv8cmast_id=local_lv8cmast_id ;

ELSIF  	local_lv7cmast_id IS NOT NULL AND
	local_lv6cmast_id IS NOT NULL AND
	local_lv5cmast_id IS NOT NULL AND
	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 4000;

    	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id,lv5ctree_id,lv6ctree_id,lv7ctree_id
    	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id,local_lv7ctree_id
    	FROM   	maxdata.lv7ctree
    	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id AND
		lv5cmast_id=local_lv5cmast_id AND
		lv6cmast_id=local_lv6cmast_id AND
		lv7cmast_id=local_lv7cmast_id ;

ELSIF  	local_lv6cmast_id IS NOT NULL AND
	local_lv5cmast_id IS NOT NULL AND
	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 5000;

	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id,lv5ctree_id,lv6ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id,local_lv6ctree_id
	FROM   	maxdata.lv6ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id AND
		lv5cmast_id=local_lv5cmast_id AND
		lv6cmast_id=local_lv6cmast_id ;

ELSIF  	local_lv5cmast_id IS NOT NULL AND
	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 6000;

	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id,lv5ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id,local_lv5ctree_id
	FROM   	maxdata.lv5ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id AND
		lv5cmast_id=local_lv5cmast_id ;

ELSIF  	local_lv4cmast_id IS NOT NULL AND
	local_lv3cmast_id IS NOT NULL AND
	local_lv2cmast_id IS NOT NULL AND
	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 7000;

	SELECT 	lv2ctree_id,lv3ctree_id,lv4ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id,local_lv4ctree_id
	FROM   	maxdata.lv4ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
		lv2cmast_id=local_lv2cmast_id AND
		lv3cmast_id=local_lv3cmast_id AND
		lv4cmast_id=local_lv4cmast_id ;

ELSIF  	local_lv3cmast_id IS NOT NULL AND
        local_lv2cmast_id IS NOT NULL AND
        local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 8000;

	SELECT 	lv2ctree_id,lv3ctree_id
	INTO   	local_lv2ctree_id,local_lv3ctree_id
	FROM   	maxdata.lv3ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
	   	lv2cmast_id=local_lv2cmast_id AND
	   	lv3cmast_id=local_lv3cmast_id ;

ELSIF  	local_lv2cmast_id IS NOT NULL AND
       	local_lv1cmast_id IS NOT NULL THEN

	n_sqlnum := 9000;

	SELECT 	lv2ctree_id
	INTO   	local_lv2ctree_id
	FROM   	maxdata.lv2ctree
	WHERE  	lv1cmast_id=local_lv1cmast_id AND
	   	lv2cmast_id=local_lv2cmast_id ;
END IF ;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        maxdata.p_log (t_proc_name, t_error_level, v_sql, t_sql2, n_sqlnum);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';
                maxdata.p_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum);
                --COMMIT;

                Raise_Application_Error(-20001,V_Sql);
END;

/
