--------------------------------------------------------
--  DDL for Procedure P_COMPLETE_MERCH_TREE2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COMPLETE_MERCH_TREE2" (
        in_lvxctree_lvl         NUMBER,         -- m_lev where to add the member (cmast level)
        in_lvctree_id           NUMBER,
        in_lvxctree_par_id      NUMBER,         -- the parent id from the corresponding ctree table
        in_lvxcmast_name        VARCHAR2,       -- name of the new member
        in_mem_cnt              NUMBER,         -- order of the member (will be attached to the member name)
        in_future_prm1          NUMBER,
        in_future_prm2          NUMBER ,
        in_future_prm3          VARCHAR2

) AS

/*
----------------------------------------------------------------------

Change History
$Log: 2416_p_complete_merch_tree2.sql,v $
Revision 1.13  2007/10/17 19:20:55  Dirapa
S0467700-- Prefix level_name to lv10mast_id for order_code column.

Revision 1.12  2007/06/19 14:40:11  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.2.10.1  2007/06/05 15:32:49  vejang
Moved from 6121 to 612HF4

Revision 1.2.8.1  2007/05/03 20:39:42  saghai
S0421349 Using Bind Variables

Revision 1.2  2006/09/19 20:30:38  saghai
Applied S0365290 - Copying lv10mast_id into order_code column

Revision 1.1  2006/09/14 20:42:15  saghai
FIXID : S0362565 Hanging nodes procedures for MDI

Revision 1.1  2006/07/25 17:57:29  joscho
FIXID : Moved from \60\oracle

Revision 1.3  2006/04/14 17:12:29  makirk
Removed import_log deletion section, added comment change history logging where needed


----------------------------------------------------------------------
*/

n_sqlnum                NUMBER(10)              := 1000;
t_proc_name             VARCHAR2(64)            := 'p_complete_merch_tree2';
t_error_level           VARCHAR2(6)             := 'info';
t_call                  VARCHAR2(1000);
t_errmsg                VARCHAR2(255)           := NULL;
t_errorcode             NUMBER(10)              := 0;
v_sql                   VARCHAR2(1000)          := '';
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);
t_cnt                   NUMBER(10);

t_lvxcmast_id           NUMBER(10)              :=  NULL;
t_lv1cmast_id           NUMBER(10)              :=  NULL;
t_lv2cmast_id           NUMBER(10)              :=  NULL;
t_lv3cmast_id           NUMBER(10)              :=  NULL;
t_lv4cmast_id           NUMBER(10)              :=  NULL;
t_lv5cmast_id           NUMBER(10)              :=  NULL;
t_lv6cmast_id           NUMBER(10)              :=  NULL;
t_lv7cmast_id           NUMBER(10)              :=  NULL;
t_lv8cmast_id           NUMBER(10)              :=  NULL;
t_lv9cmast_id           NUMBER(10)              :=  NULL;
t_lv10cat_id            NUMBER(10)              :=  NULL;
t_lv10mast_id           NUMBER(10)              :=  NULL;
t_mem_name              NVARCHAR2(100);
t_lowest_m_lev          NUMBER(1);


BEGIN

-- Log the parameters of the procedure

n_sqlnum := 2000;
t_call   :=  t_proc_name        || ' ( ' ||
             in_lvxctree_lvl    || ',' ||
             in_lvctree_id      || ',' ||
             in_lvxctree_par_id || ',' ||
             in_lvxcmast_name   || ',' ||
             in_mem_cnt || ' )';

--maxdata.ins_import_log  (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

n_sqlnum := 500;
--dbms_output.put_line (t_call);
-- THE UPPER LEVELS .. values are there in the calling proc .. code is there from the production proc. just in case.
-- update the table created in the callig proc. from lv10ctree

n_sqlnum := 600;
--  the code from here untill the --*******************************-- does not do anything
t_cnt := 1;
WHILE t_cnt <=  in_lvxctree_lvl - 1 LOOP

        n_sqlnum := n_sqlnum + t_cnt;

        BEGIN

                v_sql := 'SELECT DISTINCT lv' || t_cnt || 'cmast_id FROM maxdata.lv' || (in_lvxctree_lvl - 1) || 'ctree WHERE
                lv' || (in_lvxctree_lvl - 1)   || 'ctree_id = ' ||  in_lvxctree_par_id;

                EXECUTE IMMEDIATE v_sql INTO t_lvxcmast_id ;

                -- the IS NULL is not necessary But just in case of having any values we don't need to change them.
                v_sql := 'UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = ' || t_lvxcmast_id  || ' WHERE lvxcmast_id IS NULL and m_lev = ' || t_cnt;

                EXECUTE IMMEDIATE v_sql;

                EXCEPTION
                        WHEN others THEN
                                null;

        END;
        t_cnt := t_cnt + 1;
END LOOP;
--*******************************-- the code before this line is not needed. and will not do anything .

-- THE GIVEN LEVEL (it is given from the calling proc. basically it is in the AAHN_hanging_nodes table.

-- troubls with multiple values found.
v_sql := 'SELECT MAX(m.lv' || in_lvxctree_lvl || 'cmast_id) FROM MAXDATA.lv' ||in_lvxctree_lvl || 'cmast m JOIN MAXDATA.lv' ||in_lvxctree_lvl || 'CTREE c
                ON c.lv' ||in_lvxctree_lvl || 'cmast_id = m.lv' ||in_lvxctree_lvl || 'cmast_id
          WHERE  m.NAME =  :in_lvxcmast_name  AND c.lv' ||in_lvxctree_lvl || 'ctree_id = ' || in_lvctree_id;
--dbms_output.put_line (v_sql);
n_sqlnum := 800;

BEGIN
        EXECUTE IMMEDIATE v_sql INTO t_lvxcmast_id USING in_lvxcmast_name;

        EXCEPTION
                WHEN others THEN
                t_lvxcmast_id := NULL;
END;


UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = t_lvxcmast_id WHERE m_lev = in_lvxctree_lvl;

-- THE LOWER LEVELS
n_sqlnum := 1200;

t_cnt := in_lvxctree_lvl + 1;
SELECT COUNT(1) - 1 INTO t_lowest_m_lev FROM maxdata.sess_complete_merch_tree;

--RETURN;
WHILE t_cnt <= t_lowest_m_lev LOOP

        maxapp.p_get_next_key (t_cnt, 9, 1, t_lvxcmast_id, t_errmsg);

        IF t_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
        END IF;

        SELECT   m_name  INTO t_mem_name FROM  MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = t_cnt;

        v_sql := 	'INSERT INTO maxdata.lv' || t_cnt || 'cmast (lv' || t_cnt ||'cmast_id , name, lv'|| t_cnt ||
        		'cmast_userid ) values ( ' || t_lvxcmast_id  ||
        		',''[''||'||':t_mem_name'||'||'' '' ||' || in_mem_cnt||
        		'||'']'', SUBSTR(:t_mem_name,1,15) '||'||''-''||' || t_lvxcmast_id  ||')';

        n_sqlnum := 1300;

        EXECUTE IMMEDIATE v_sql USING t_mem_name,t_mem_name;


        v_sql := 'UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = ' || t_lvxcmast_id  || ' WHERE m_lev = ' || t_cnt;

        n_sqlnum := 1500;

        EXECUTE IMMEDIATE v_sql;

        t_cnt := t_cnt + 1;

--dbms_output.put_line (t_cnt);
--dbms_output.put_line (t_lvxcmast_id);

--RETURN;
END LOOP;

BEGIN
        -- GET t_lv10cat_id for the new created ctree.
        SELECT  lvxcmast_id INTO t_lv1cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 1;
        SELECT  lvxcmast_id INTO t_lv2cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 2;
        SELECT  lvxcmast_id INTO t_lv3cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 3;
        SELECT  lvxcmast_id INTO t_lv4cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 4;
        SELECT  lvxcmast_id INTO t_lv5cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 5;
        SELECT  lvxcmast_id INTO t_lv6cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 6;
        SELECT  lvxcmast_id INTO t_lv7cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 7;
        SELECT  lvxcmast_id INTO t_lv8cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 8;
        SELECT  lvxcmast_id INTO t_lv9cmast_id  FROM MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 9;

        EXCEPTION
                WHEN others THEN
                null;
END;


-- reuse the lv10cat if exists
BEGIN
    SELECT MAX(lv10cat_id) INTO t_lv10cat_id
    FROM maxdata.lv10cat
    WHERE (lv9cmast_id = t_lv9cmast_id OR (lv9cmast_id IS NULL AND t_lowest_m_lev < 9))
    AND   (lv8cmast_id = t_lv8cmast_id OR (lv8cmast_id IS NULL AND t_lowest_m_lev < 8))
    AND   (lv7cmast_id = t_lv7cmast_id OR (lv7cmast_id IS NULL AND t_lowest_m_lev < 7))
    AND   (lv6cmast_id = t_lv6cmast_id OR (lv6cmast_id IS NULL AND t_lowest_m_lev < 6))
    AND   (lv5cmast_id = t_lv5cmast_id OR (lv5cmast_id IS NULL AND t_lowest_m_lev < 5))
    AND   (lv4cmast_id = t_lv4cmast_id OR (lv4cmast_id IS NULL AND t_lowest_m_lev < 4))
    AND   (lv3cmast_id = t_lv3cmast_id OR (lv3cmast_id IS NULL AND t_lowest_m_lev < 3))
    AND   (lv2cmast_id = t_lv2cmast_id OR (lv2cmast_id IS NULL AND t_lowest_m_lev < 2))
    AND   lv1cmast_id > 0  AND lv10cat_id > 0;

    EXCEPTION
            WHEN others THEN
            t_lv10cat_id := null;
END;
/*
dbms_output.put_line ('t_lv9cmast_id');
dbms_output.put_line (t_lv9cmast_id);
dbms_output.put_line (t_lv8cmast_id);
dbms_output.put_line (t_lv7cmast_id);
dbms_output.put_line (t_lv6cmast_id);
dbms_output.put_line (t_lv5cmast_id);
dbms_output.put_line (t_lv4cmast_id);
dbms_output.put_line (t_lv3cmast_id);
dbms_output.put_line (t_lv2cmast_id);
dbms_output.put_line (t_lv1cmast_id);
dbms_output.put_line ('end print');
*/

--dbms_output.put_line (t_lv10cat_id);

IF t_lv10cat_id IS NULL THEN

        n_sqlnum := 1600;
        maxapp.p_get_next_key (10, 3, 1,  t_lv10cat_id  , t_errmsg  );
        IF t_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
        END IF;

        n_sqlnum := 1700;

        INSERT INTO maxdata.lv10cat (lv10cat_id, lv1cmast_id, lv2cmast_id, lv3cmast_id, lv4cmast_id, lv5cmast_id,
                                    lv6cmast_id, lv7cmast_id, lv8cmast_id, lv9cmast_id , record_type )
        VALUES (t_lv10cat_id,t_lv1cmast_id, t_lv2cmast_id, t_lv3cmast_id, t_lv4cmast_id, t_lv5cmast_id,
                t_lv6cmast_id, t_lv7cmast_id, t_lv8cmast_id, t_lv9cmast_id , 'M');
END IF;



-- insert new lv10mast rec.
maxapp.p_get_next_key (10, 1, 1, t_lv10mast_id, t_errmsg);
IF t_errmsg IS NOT NULL THEN
        RAISE_APPLICATION_ERROR (-20001, 'P_GET_NEXT_KEY error - ' || t_errmsg);
END IF;

SELECT  m_name INTO t_mem_name FROM  MAXDATA.SESS_COMPLETE_MERCH_TREE WHERE m_lev = 10;

v_sql := 	'INSERT INTO maxdata.lv10mast (lv10mast_id , name, lv10cat_id , record_type, order_code ) values( ' ||
		t_lv10mast_id  || ', ''[''||'||':t_mem_name' ||'||'' ''||'||in_mem_cnt||'||'']-''||'|| t_lv10mast_id  ||',' ||
		t_lv10cat_id || ' ,''M'','|| 'LTRIM(RTRIM(SUBSTR(' ||  'LTRIM(RTRIM(SUBSTR(:t_mem_name,1,40))) ' || ' || ' || '''' || '-' || '''' || ' || ' || 'CAST(' || t_lv10mast_id || ' AS VARCHAR2(10))' ||  ',1,50)))'  ||') ';

n_sqlnum := 1800;
--dbms_output.put_line (v_sql);
EXECUTE IMMEDIATE v_sql USING t_mem_name,t_mem_name;

COMMIT;



--END;


EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := SUBSTR(v_sql,1,255);
                        --maxdata.ins_import_log (t_proc_name, t_error_level, t_sql2, t_sql3, n_sqlnum, NULL);
                END IF;

                -- Log the error message
                t_error_level := 'error';
                v_sql := SQLERRM || ' (' || t_call ||
                                ', SQL#:' || n_sqlnum || ')';

                --maxdata.ins_import_log (t_proc_name, t_error_level, v_sql, NULL, n_sqlnum, NULL);
                COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);


END;

/
