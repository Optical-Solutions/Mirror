--------------------------------------------------------
--  DDL for Procedure P_COMPLETE_MERCH_TREE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COMPLETE_MERCH_TREE" (
        in_merch_lvl            NUMBER,         -- m_lev where to add the member (cmast level). Must be 2 <= and <= 9
        in_lvxctree_par_id      NUMBER,         -- the parent id from the corresponding ctree table NOT NULL
        in_lvxcmast_name        NVARCHAR2,      -- name of the new member NOT NULL
        in_mem_cnt              NUMBER,         -- member number that will be attached to the member name >= 1
        in_lv10mast_id          NUMBER,         -- This will be generated in the calling proc using next_key.
        in_future_prm2          NUMBER,         -- (-1)
        in_future_prm3          NUMBER       -- NULL

) AS

/*
----------------------------------------------------------------------
Change History:
$Log: 2358_p_complete_merch_tree.sql,v $
Revision 1.21.4.1  2008/09/03 01:16:04  saghai
612-HF13 (HBC) change Re-wrote query for performance improvement

Revision 1.21  2007/10/17 19:20:57  Dirapa
S0467700-- Prefix level_name to lv10mast_id for order_code column.

Revision 1.20  2007/06/19 14:38:57  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.14  2006/09/26 17:57:14  saghai
S0382440 Always insert new lvxcmast record

Revision 1.13  2006/09/12 14:15:00  saghai
S0375572 Changed datatype of in_future_prm3 to NUMBER

Revision 1.12  2006/07/05 17:50:12  saghai
S0365290 - Copying lv10mast_id into order_code column

Revision 1.11  2006/06/07 22:39:52  saghai
No comment given.

Revision 1.10  2006/05/31 18:03:57  saghai
S0354052 When member count = 1 then do not add the number '1' in the name and userid.

Revision 1.9  2006/04/14 19:32:36  makirk
Removed import_log cleanup section

Revision 1.8  2006/03/07 16:13:24  healja
Issue s0347411. reported only for Migration
The procedure should check for the lowest level and figure out if there is enough lvXcmast_ids to pick up the right Lv10cat.
Review : Satish, Diwakar


Revision 1.7  2006/01/06 15:05:32  healja
adding functionalities to generate unique names in the procedure in case the give name is NULL.
Review Diwakar

Revision 1.6  2005/12/14 19:09:16  healja
commenting out the import_log.
correcting some cast phrases.. adding some debug code

Revision 1.5  2005/12/09 15:42:25  healja
Adding support for level10 insert(lv10mast).
Making code more SQL compliant for portability.

Revision 1.4  2005/11/02 20:57:42  healja
session table name change.
fixing little bug for lv1 DSQL change from cmast to ctree.
Review Mark

Revision 1.3  2005/10/17 20:52:50  healja
getting an out param for the application and all changes required for it

Revision 1.2  2005/09/23 18:17:20  healja
adding error handling for params.

Revision 1.1  2005/09/21 20:56:37  joscho
FIXID : Helmi's hanging node code for App


Description:

Called internally by p_complete_merch_tree_app in order to complete a merch tree
starting from a given level/id of a parent of cmast down to SKU.

----------------------------------------------------------------------
*/

n_sqlnum                NUMBER(10)          := 1000;
t_proc_name             VARCHAR2(25)        := 'p_complete_merch_tree';
t_error_level           VARCHAR2(6)         := 'info';
t_call                  VARCHAR2(1000);
t_errmsg                VARCHAR2(255)       := NULL;
t_errorcode             NUMBER(10)          := 0;
v_sql                   VARCHAR2(1000)      := '';
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);
t_cnt                   NUMBER(10);

t_lvxcmast_id           NUMBER(10)          :=  NULL;
t_lv10cat_id            NUMBER(10)          :=  NULL;
t_lowest_m_lev          NUMBER(1);
t_lvxcmast_name         VARCHAR2(100);
t_parent_exists		NUMBER(10);

BEGIN

-- Log the parameters of the procedure
n_sqlnum := 200;
t_call := t_proc_name || ' (' ||
        CAST(COALESCE(in_merch_lvl , -123) AS VARCHAR2) || ',' ||
        CAST(COALESCE(in_lvxctree_par_id , -123) AS VARCHAR2) || ',' ||
        COALESCE(in_lvxcmast_name, 'NULL') || ', ' ||
        CAST(COALESCE(in_mem_cnt , -123) AS VARCHAR2) || ', ' ||
        CAST(COALESCE(in_lv10mast_id , -123) AS VARCHAR2) || ', ' ||
        CAST(COALESCE(in_future_prm2 , -123) AS VARCHAR2) || ', ' ||
        CAST(COALESCE(in_future_prm3 , -123) AS VARCHAR2) || ')';
--maxdata.ins_import_log  (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

-- Check for param validation and
-- Find out how many levels we have to go down, excluding level 10, hence (-1).
SELECT  500, COUNT(1) - 1 ,
        CASE WHEN in_lvxctree_par_id IS NULL OR in_mem_cnt < 1
        OR in_merch_lvl < 2 OR in_merch_lvl > 10 THEN
                        'Wrong Parameters: ' || t_call
        ELSE NULL END
INTO n_sqlnum, t_lowest_m_lev, v_sql
FROM MAXDATA.SESS_COMPLETE_MERCH_TREE;

IF v_sql IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20001 ,v_sql);
END IF;
-- THE UPPER LEVELS
-- update the table created in the callig proc.
--dbms_output.put_line ('t_lowest_m_lev');
--dbms_output.put_line (t_lowest_m_lev);

n_sqlnum := 600;
t_cnt    := 1;
WHILE t_cnt <=  in_merch_lvl - 1 LOOP
BEGIN

-- if the new node is on level 2 there is no ctree parent.
-- for the case of inserting product we could have any level as parent level(the lowest level).
-- for other cases where inserting merch levels higher than products.
        SELECT CASE in_merch_lvl
                WHEN 2 THEN
                        'SELECT DISTINCT lv1cmast_id FROM maxdata.lv2ctree'
                WHEN 10 THEN
                        'SELECT DISTINCT lv' || CAST(t_cnt AS VARCHAR2(10)) || 'cmast_id
                        FROM maxdata.lv10ctree WHERE
                        lv' || CAST(t_lowest_m_lev AS VARCHAR2(10)) || 'ctree_id = ' || CAST(in_lvxctree_par_id AS VARCHAR2(10))
                ELSE
                        'SELECT DISTINCT lv' || CAST(t_cnt AS VARCHAR2(10)) || 'cmast_id
                        FROM maxdata.lv'|| CAST((in_merch_lvl - 1)  AS VARCHAR2(10)) ||'ctree WHERE
                        lv' || CAST((in_merch_lvl - 1) AS VARCHAR2(10)) || 'ctree_id = ' || CAST(in_lvxctree_par_id AS VARCHAR2(10))
                END
                INTO v_sql
        FROM DUAL;

        --dbms_output.put_line (v_sql);
        EXECUTE IMMEDIATE v_sql INTO t_lvxcmast_id  ;

        v_sql := 'UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = ' || CAST(COALESCE(t_lvxcmast_id , 0) AS VARCHAR2) ||
                ' WHERE lvxcmast_id IS NULL and m_lev = ' || CAST(t_cnt  AS VARCHAR2) ;

        --dbms_output.put_line (v_sql);
        EXECUTE IMMEDIATE v_sql;

        SELECT  n_sqlnum + t_cnt , t_cnt + 1
        INTO n_sqlnum ,  t_cnt
        FROM DUAL;

END;
END LOOP;

-- THE GIVEN LEVEL
-- See if the name exists in the cmast table
-- Use MAX just in case there is more than one rows with the same name.
-- in the case in_merch_lvl = 10 we will get an error and that will have the t_lvxcmast_id as null.. which is OK.

SELECT 700,  'SELECT MAX(lv' || CAST(in_merch_lvl AS VARCHAR2(10)) || 'cmast_id) FROM MAXDATA.lv' || CAST(in_merch_lvl AS VARCHAR2(10)) || 'cmast
                WHERE NAME =  ''' || in_lvxcmast_name || ''' OR NAME = ''' || '[' || in_lvxcmast_name || ' ' ||  CAST(in_mem_cnt AS VARCHAR2(10)) ||']'''
INTO n_sqlnum, v_sql
FROM DUAL;
--dbms_output.put_line (v_sql);
BEGIN
        EXECUTE IMMEDIATE v_sql INTO t_lvxcmast_id;

        EXCEPTION
                WHEN others THEN
                t_lvxcmast_id := NULL;
END;

--dbms_output.put_line ('t_lvxcmast_id');


-- The code below is commented out as it is not necessary
-- A new lvxcmast record will be created everytime.
-- No need to check for add unique
-----------------------------------------------------------------------------------
/*
BEGIN
        SELECT DISTINCT 800, add_unique
        INTO n_sqlnum, t_cnt
        FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = in_merch_lvl;
        EXCEPTION
                WHEN others THEN
                t_cnt := NULL;
END;
*/
------------------------------------------------------------------------------------
IF in_mem_cnt > 1 THEN
	t_lvxcmast_name := '['||in_lvxcmast_name || ' ' || CAST(in_mem_cnt AS VARCHAR2)||']' ;
ELSE
	t_lvxcmast_name := '['||in_lvxcmast_name||']';
END IF;


-- The code below is commented out as it is not necessary
-- A new lvxcmast record will be created everytime.
----------------------------------------------------------------------------------------------------
-- If add_unique flag is off
-- Check if the in_lvxctree_par_id exists in the lvxctree table
-- If it exists raise an error.

/*IF (t_cnt IS NULL OR t_cnt = 0) AND t_lvxcmast_id IS NOT NULL THEN
BEGIN
	SELECT 850, 'SELECT COUNT(*) FROM maxdata.lv' || CAST(in_merch_lvl AS VARCHAR2(10)) || 'ctree '||
	' WHERE lv'||CAST(in_merch_lvl AS VARCHAR2(10)) || 'cmast_id ='||CAST(t_lvxcmast_id AS VARCHAR2(10))||
	' AND lv'||CAST((in_merch_lvl - 1)  AS VARCHAR2(10)) || CASE WHEN in_merch_lvl = 2
								THEN 'cmast_id'
								ELSE 'ctree_id'
								END
							||' = '||CAST(in_lvxctree_par_id AS VARCHAR2(10))
        INTO n_sqlnum, v_sql
        FROM dual;

        dbms_output.put_line (v_sql);
        EXECUTE IMMEDIATE v_sql INTO t_parent_exists;

        IF t_parent_exists > 0 THEN
        	RAISE_APPLICATION_ERROR (-20001,'[[DT_ADD_MEMB_DUP]]');
        END IF;
END;
END IF;
*/
------------------------------------------------------------------------------------------------
-- insert new lvxcmast record.
n_sqlnum:= 870;

IF in_merch_lvl < 10 THEN
        --  The name is from the parameter. We generate ID.
        maxapp.p_get_next_key (in_merch_lvl, 9, 1, t_lvxcmast_id, t_errmsg);

        IF t_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
        END IF;

        SELECT 900, 'INSERT INTO maxdata.lv' || CAST(in_merch_lvl AS VARCHAR2(10)) || 'cmast
                (lv' || CAST(in_merch_lvl AS VARCHAR2(10)) ||'cmast_id , name, lv' || CAST(in_merch_lvl AS VARCHAR2(10)) ||'cmast_userid, record_type  )
                values ( ' || CAST(t_lvxcmast_id AS VARCHAR2(10)) || ',''' || t_lvxcmast_name || ''',''' ||
                substr(t_lvxcmast_name,1,15) ||'-' || CAST(t_lvxcmast_id AS VARCHAR2(10)) || ''' ,''M'') '
        INTO n_sqlnum, v_sql
        FROM dual;

        --dbms_output.put_line (v_sql);
        EXECUTE IMMEDIATE v_sql;
END IF;

-- Update the tmp table with the existing or newly-generated ID.
-- if in_merch_lvl = 10 we will update but anyway we are not going to use it.
SELECT  1100, 'UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = ' ||
                CAST(COALESCE(t_lvxcmast_id, 0) AS VARCHAR2(10)) || ' WHERE m_lev = ' || CAST(in_merch_lvl AS VARCHAR2(10))
INTO n_sqlnum, v_sql
FROM DUAL;

EXECUTE IMMEDIATE v_sql;

-- THE LOWER LEVELS
-- We start from the given level + 1 and loop to 9.
SELECT 1200, in_merch_lvl + 1
INTO n_sqlnum, t_cnt
FROM DUAL;

-- Loop down through all the lower levels.
-- this will not get executed when in_merch_lvl = 10 .
WHILE t_cnt <= t_lowest_m_lev LOOP
BEGIN
        maxapp.p_get_next_key (t_cnt, 9, 1,  t_lvxcmast_id, t_errmsg);

        IF t_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
        END IF;

        -- Compose a name like 'Style 1', 'Style 2', and so on.
        SELECT 1300 , 'INSERT INTO maxdata.lv' || CAST(t_cnt AS VARCHAR2(10)) || 'cmast
                (lv' || CAST(t_cnt AS VARCHAR2(10)) ||'cmast_id , name, lv' || CAST(t_cnt AS VARCHAR2(10)) ||'cmast_userid, record_type)
                values ( ' || CAST(t_lvxcmast_id AS VARCHAR2(10)) ||
                ','|| 	CASE WHEN in_mem_cnt > 1
                	THEN  '''[' || m_name || ' ' || CAST(in_mem_cnt AS VARCHAR2(10)) || ']'''
                	ELSE '''['||m_name||']'''
                	END ||
                ','||
                	CASE WHEN in_mem_cnt > 1
                	THEN '''[' || substr(m_name,1,15) || ' ' || CAST(in_mem_cnt AS VARCHAR2(10))|| ']-' || CAST(t_lvxcmast_id AS VARCHAR2(10)) || ''''
                	ELSE '''['||substr(m_name,1,15)|| ']-' || CAST(t_lvxcmast_id AS VARCHAR2(10))||''''
                	END ||
		 ',''M'') '
        INTO n_sqlnum, v_sql
        FROM maxdata.sess_complete_merch_tree
        WHERE m_lev = t_cnt;

--dbms_output.put_line (v_sql);

        EXECUTE IMMEDIATE v_sql;

        SELECT 1500, 'UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = ' || CAST(t_lvxcmast_id AS VARCHAR2(10)) ||
                ' WHERE m_lev = ' || CAST(t_cnt AS VARCHAR2(10)), t_cnt + 1
        INTO n_sqlnum, v_sql,  t_cnt
        FROM dual;

        EXECUTE IMMEDIATE v_sql;
--dbms_output.put_line (t_lvxcmast_id);
END; -- while begin
END LOOP;

-- Find out LV10CAT ID. There would be cases that LV10CAT exists even with a hanging node.
-- In that case, re-use LV10CAT ID. the max in case of any data issues (normally duplicate lvxcmast are not allowed)
BEGIN
SELECT 1550, MAX(lv10cat_id)
INTO n_sqlnum, t_lv10cat_id
FROM maxdata.lv10cat
WHERE   (lv1cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 1))
 AND    (lv2cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 2) OR (lv2cmast_id IS NULL AND t_lowest_m_lev < 2))
 AND    (lv3cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 3) OR (lv3cmast_id IS NULL AND t_lowest_m_lev < 3))
 AND    (lv4cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 4) OR (lv4cmast_id IS NULL AND t_lowest_m_lev < 4))
 AND    (lv5cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 5) OR (lv5cmast_id IS NULL AND t_lowest_m_lev < 5))
 AND    (lv6cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 6) OR (lv6cmast_id IS NULL AND t_lowest_m_lev < 6))
 AND    (lv7cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 7) OR (lv7cmast_id IS NULL AND t_lowest_m_lev < 7))
 AND    (lv8cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 8) OR (lv8cmast_id IS NULL AND t_lowest_m_lev < 8))
 AND    (lv9cmast_id = (SELECT  lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 9) OR (lv9cmast_id IS NULL AND t_lowest_m_lev < 9))
 AND  lv10cat_id > 0;


EXCEPTION
        WHEN others THEN
        t_lv10cat_id := null;
END;

--dbms_output.put_line ('t_lv10cat_id');
--dbms_output.put_line (t_lv10cat_id);

-- If there is no corresponding LV10CAT ID, then insert one.
IF t_lv10cat_id IS NULL THEN
BEGIN
        n_sqlnum := 1600;
        maxapp.p_get_next_key (10, 3, 1,  t_lv10cat_id, t_errmsg);
        IF t_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
        END IF;
--dbms_output.put_line (t_lv10cat_id);
        n_sqlnum := 1700;

        INSERT INTO MAXDATA.LV10CAT (lv10cat_id, lv1cmast_id, lv2cmast_id, lv3cmast_id, lv4cmast_id, lv5cmast_id,
                                        lv6cmast_id, lv7cmast_id, lv8cmast_id, lv9cmast_id , record_type )
        VALUES (t_lv10cat_id,
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 1),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 2),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 3),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 4),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 5),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 6),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 7),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 8),
        (SELECT lvxcmast_id FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 9),
        'M');
END;
END IF;

--dbms_output.put_line ('t_lv10cat_id');
--dbms_output.put_line (t_lv10cat_id);

-- Insert a new lv10mast.
-- we need to specify the record_type as M. triggers will update corresponding ctree/cmast tables.
SELECT 1800 , 'INSERT INTO maxdata.lv10mast
                (lv10mast_id , name, lv10cat_id , record_type, order_code ) values
                (' || CAST(in_lv10mast_id AS VARCHAR2(10)) || ',' ||
	       	CASE WHEN in_mem_cnt > 1
                	THEN  '''[' ||         CASE in_merch_lvl
        		      	   	     WHEN 10 THEN CAST(in_lvxcmast_name AS VARCHAR2(100))
				     ELSE m_name END
         	|| ' ' || CAST(in_mem_cnt AS VARCHAR2(10)) || ']-' || CAST(in_lv10mast_id AS VARCHAR2(10))||''''
                	ELSE '''['||        CASE in_merch_lvl
        		     		 WHEN 10 THEN CAST(in_lvxcmast_name AS VARCHAR2(100))
        				 ELSE m_name END
		 || ']-' || CAST(in_lv10mast_id AS VARCHAR2(10))||''''
                	END
	       || ',' ||
                CAST(t_lv10cat_id AS VARCHAR2(10)) || ',''M'',' ||  'LTRIM(RTRIM(SUBSTR(' || '''' || LTRIM(RTRIM(SUBSTR(m_name,1,40))) || '-' || CAST(in_lv10mast_id AS VARCHAR2(10)) || '''' || ',1,50)))' ||') '
INTO n_sqlnum, v_sql
FROM  MAXDATA.SESS_COMPLETE_MERCH_TREE
WHERE m_lev = 10;

 --dbms_output.put_line (v_sql);

EXECUTE IMMEDIATE (v_sql);

COMMIT;
RETURN;

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

                maxdata.ins_import_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum, NULL);
                COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COMPLETE_MERCH_TREE" TO "MADMAX";
