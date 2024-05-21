--------------------------------------------------------
--  DDL for Procedure P_COMPLETE_MERCH_TREE_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COMPLETE_MERCH_TREE_LOAD" 
AS

/*
------------------------------------------------------------------------

Change History
$Log: 2418_p_complete_merch_tree_load.sql,v $
Revision 1.7  2007/06/19 14:38:45  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.2  2006/09/19 20:29:21  saghai
changed datatype length

Revision 1.1  2006/09/14 20:42:14  saghai
S0362565 Hanging nodes procedures for MDI

Revision 1.1  2006/07/25 17:57:32  joscho
FIXID : Moved from \60\oracle

Revision 1.4  2006/06/20 16:22:40  dirapa
No comment given.

Revision 1.3  2006/04/27 20:44:58  dirapa
--#S0356466

Removed hard coded value for lv1cmast_id from update session table statement.

Revision 1.2  2006/04/14 17:12:27  makirk
Removed import_log deletion section, added comment change history logging where needed



V5.6.3
--

-------------------------------------------------------------------------
*/

n_sqlnum                NUMBER(10)              := 1000;
t_proc_name             VARCHAR2(30)            := 'p_complete_merch_tree_load';
t_error_level           VARCHAR2(6)             := 'info';
t_call                  VARCHAR2(1000);
t_errmsg                VARCHAR2(255)           := NULL;
t_errorcode             NUMBER(10)              := 0;
v_sql                   VARCHAR2(4000)          := '';
v_sql2                  VARCHAR2(4000)          := '';
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);
t_cnt                   NUMBER(10);
t_lowest_level          NUMBER(10);
t_node_name             NVARCHAR2(100);
t_merch_lev             NUMBER(2);
t_par_id                NUMBER(10);

-- for template
t_hang_lev              NUMBER(2);
t_mem_id                NUMBER(10);

I                       NUMBER (3);
J                       NUMBER (3);
t_col_value             VARCHAR2(1000);
t_mplan_tbl_nm          VARCHAR2(100);





BEGIN
-- Log the parameters of the procedure
n_sqlnum := 200;
t_call   := t_proc_name;

--maxdata.ins_import_log  (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);

n_sqlnum := 500;


DELETE FROM maxdata.AAHN_hanging_nodes;

SELECT COUNT(1) - 1 INTO t_lowest_level
FROM maxdata.hier_level
WHERE  hier_id = 11;


INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 2 merch_lev, lv2ctree_id, c.lv1cmast_id par_id, c.lv2cmast_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, m.name mem_name
FROM maxdata.lv2ctree c
JOIN maxdata.lv2cmast m ON m.lv2cmast_id = c.lv2cmast_id
WHERE NOT EXISTS (SELECT * FROM maxdata.lv3ctree ll  WHERE ll.lv2ctree_id = c.lv2ctree_id  AND t_lowest_level > 2)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv2ctree_id = c.lv2ctree_id AND active_lkup <> 0 AND t_lowest_level = 2);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 3 merch_lev, lv3ctree_id, c.lv2ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, NULL, NULL, NULL, NULL, NULL, NULL, m.name mem_name
FROM maxdata.lv3ctree c
JOIN maxdata.lv3cmast m ON m.lv3cmast_id = c.lv3cmast_id
WHERE NOT EXISTS (SELECT * FROM maxdata.lv4ctree ll  WHERE ll.lv3ctree_id  = c.lv3ctree_id AND t_lowest_level > 3)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv3ctree_id = c.lv3ctree_id AND active_lkup <> 0 AND t_lowest_level = 3);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 4 merch_lev, lv4ctree_id, c.lv3ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, NULL, NULL, NULL, NULL, NULL, m.name mem_name
FROM maxdata.lv4ctree c
JOIN maxdata.lv4cmast m ON m.lv4cmast_id = c.lv4cmast_id
WHERE NOT EXISTS (SELECT * FROM maxdata.lv5ctree ll  WHERE ll.lv4ctree_id  = c.lv4ctree_id AND t_lowest_level > 4)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv4ctree_id = c.lv4ctree_id AND active_lkup <> 0 AND t_lowest_level = 4);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 5 merch_lev, lv5ctree_id, c.lv4ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, c.lv5cmast_id, NULL, NULL, NULL, NULL, m.name mem_name
FROM maxdata.lv5ctree c
JOIN maxdata.lv5cmast m ON M.LV5cmast_id = c.lv5cmast_ID
WHERE NOT EXISTS (SELECT * FROM maxdata.lv6ctree ll  WHERE ll.lv5ctree_id  = c.lv5ctree_id AND t_lowest_level > 5)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv5ctree_id = c.lv5ctree_id AND active_lkup <> 0 AND t_lowest_level = 5);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 6 merch_lev, lv6ctree_id, c.lv5ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, c.lv5cmast_id, c.lv6cmast_id, NULL, NULL, NULL, m.name mem_name
FROM maxdata.lv6ctree c
JOIN maxdata.lv6cmast m ON m.lv6cmast_id = c.lv6cmast_id
WHERE NOT EXISTS (SELECT * FROM maxdata.lv7ctree ll  WHERE ll.lv6ctree_id  = c.lv6ctree_id AND t_lowest_level > 6)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv6ctree_id = c.lv6ctree_id AND active_lkup <> 0 AND t_lowest_level = 6);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 7 merch_lev, lv7ctree_id, c.lv6ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, c.lv5cmast_id, c.lv6cmast_id, c.lv7cmast_id, NULL, NULL, m.name mem_name
FROM maxdata.lv7ctree c
JOIN maxdata.lv7cmast m ON M.LV7cmast_id = c.lv7cmast_ID
WHERE NOT EXISTS (SELECT * FROM maxdata.lv8ctree ll  WHERE ll.lv7ctree_id  = c.lv7ctree_id AND t_lowest_level > 7)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv7ctree_id = c.lv7ctree_id AND active_lkup <> 0 AND t_lowest_level = 7);

INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 8 merch_lev, lv8ctree_id, c.lv7ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, c.lv5cmast_id, c.lv6cmast_id, c.lv7cmast_id, c.lv8cmast_id, NULL, m.name mem_name
FROM maxdata.lv8ctree c
JOIN maxdata.lv8cmast m ON M.LV8cmast_id = c.lv8cmast_ID
WHERE NOT EXISTS (SELECT * FROM maxdata.lv9ctree ll  WHERE ll.lv8ctree_id  = c.lv8ctree_id AND t_lowest_level > 8)
AND   NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv8ctree_id = c.lv8ctree_id AND active_lkup <> 0 AND t_lowest_level = 8);


INSERT INTO maxdata.AAHN_hanging_nodes
SELECT 9 merch_lev, lv9ctree_id, c.lv8ctree_id par_id, c.lv2cmast_id, c.lv3cmast_id, c.lv4cmast_id, c.lv5cmast_id, c.lv6cmast_id, c.lv7cmast_id, c.lv8cmast_id, c.lv9cmast_id, m.name mem_name
FROM maxdata.lv9ctree c
JOIN maxdata.lv9cmast m ON m.lv9cmast_id = c.lv9cmast_id
WHERE NOT EXISTS (SELECT * FROM maxdata.lv10mast l10 WHERE l10.lv9ctree_id = c.lv9ctree_id AND active_lkup <> 0 AND t_lowest_level = 9);



-- not needed will not hurt... just for the case to verify bad data
--v_sql := 'create table maxdata.mig61_AAHN_hanging_nodes as select * from maxdata.AAHN_hanging_nodes';
--EXECUTE IMMEDIATE v_sql;

n_sqlnum := 600;

DECLARE CURSOR hang_cur IS
SELECT DISTINCT * FROM maxdata.AAHN_hanging_nodes
WHERE PAR_ID IS NOT NULL
ORDER BY MERCH_LEV, lv9cmast_id, lv8cmast_id, lv7cmast_id, lv6cmast_id, lv5cmast_id, lv4cmast_id, lv3cmast_id, lv2cmast_id;
BEGIN
FOR c in hang_cur LOOP
BEGIN

        n_sqlnum := n_sqlnum + 1;
        DELETE FROM maxdata.SESS_COMPLETE_MERCH_TREE;

        -- GET THE MERCH/CMAST IDS INTO THE TMP TABLE.
        INSERT INTO maxdata.SESS_COMPLETE_MERCH_TREE
        SELECT level_id - 10 m_lev, level_name m_name, NULL  lvxcmast_id, NULL add_unique
        FROM maxdata.hier_level
        WHERE  hier_id = 11; -- cmast hier_id

        BEGIN

                -- this will fill up some levels (down to the given level.. we sould not update this in the main proc .. there is where clause to stop that.
                UPDATE MAXDATA.SESS_COMPLETE_MERCH_TREE SET lvxcmast_id = (SELECT lv1cmast_id FROM maxdata.lv1cmast
				       				        	WHERE lv1cmast_id IN
										     	  (SELECT lv1cmast_id FROM maxdata.lv2ctree
											   WHERE lv2cmast_id = c.lv2cmast_id
											   )
									  )
		       				         WHERE m_lev = 1;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv2cmast_id WHERE m_lev = 2;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv3cmast_id WHERE m_lev = 3;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv4cmast_id WHERE m_lev = 4;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv5cmast_id WHERE m_lev = 5;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv6cmast_id WHERE m_lev = 6;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv7cmast_id WHERE m_lev = 7;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv8cmast_id WHERE m_lev = 8;
                UPDATE maxdata.sess_complete_merch_tree SET lvxcmast_id = c.lv9cmast_id WHERE m_lev = 9;

                exception
                        WHEN others THEN
                        NULL;
        END;


        SELECT MAX(m_lev) INTO t_merch_lev FROM maxdata.sess_complete_merch_tree WHERE m_lev < 10 AND lvxcmast_id IS NOT NULL;

        t_cnt := 1;

        maxdata.p_complete_merch_tree2 (
                t_merch_lev     ,
                c.merch_id      ,
                c.par_id        ,
                c.mem_name      ,
                t_cnt           ,
                -1              ,
                -1              ,
                '-1')           ;

        --DELETE FROM maxdata.SESS_COMPLETE_MERCH_TREE;
END;
END LOOP;
END;



n_sqlnum := 700;
-- TEMPLATES AND MPLAN TABLES.

--- finding all templates (from/to)who has hanging members.

DECLARE CURSOR hang_tmpl_cur IS
        SELECT DISTINCT h.merch_lev, h.merch_id, t.template_id, t.to_level - 10 to_level FROM AAHN_hanging_nodes h
        JOIN maxdata.dimset_template t
        ON  h.merch_lev  >=  t.from_level - 10 AND h.merch_lev < t.to_level - 10
        JOIN maxdata.dimset_template_lev l
        ON t.template_id = l.template_id AND h.merch_lev = l.level_number - 10
        JOIN maxdata.dimset_template_mem m
        ON t.template_id = m.template_id AND l.level_number = m.level_number AND h.merch_id = m.member_id
        WHERE   t.dimension_type = 2 AND
                l.level_incl_flag = 1 AND l.dynamic_flag <> 1
        ORDER BY t.template_id, h.merch_lev, h.merch_id;

BEGIN
FOR c in hang_tmpl_cur LOOP
BEGIN
        n_sqlnum := n_sqlnum + 1;
        -- get the level next to the hanging (first generation child to be inserted)
        t_hang_lev := c.merch_lev + 1;

        WHILE t_hang_lev <= C.to_level LOOP
                -- we suppose to have only one member (the one added to the hanging as child)
                v_sql := 'SELECT lv' || t_hang_lev || 'ctree_id, m.name, lv' || (t_hang_lev - 1 ) || 'ctree_id
                          FROM maxdata.lv' || t_hang_lev || 'ctree c
                          JOIN lv' || t_hang_lev || 'cmast m on c.lv' || t_hang_lev || 'cmast_id = m.lv' || t_hang_lev || 'cmast_id
                          WHERE lv' || C.MERCH_LEV  || 'ctree_id  = ' || c.merch_id;

                IF t_hang_lev = 10 THEN
                        v_sql := 'SELECT lv10mast_id, name, lv9ctree_id
                                  FROM maxdata.lv10mast
                                  WHERE lv' || C.MERCH_LEV  || 'ctree_id = ' || c.merch_id;
                END IF;

                BEGIN

                        EXECUTE IMMEDIATE v_sql INTO t_mem_id, t_node_name ,t_par_id;

                        INSERT INTO maxdata.dimset_template_mem (member_id, member_name, template_id, level_number ,PARENT_MEMBER_ID )
                        VALUES(t_mem_id, t_node_name, C.template_id ,(t_hang_lev + 10),t_par_id );

                        EXCEPTION
                                WHEN others THEN
                                NULL;
                END;

                t_hang_lev := t_hang_lev + 1;

        END LOOP;

END;
END LOOP;
END;

n_sqlnum := 8000000;


BEGIN
DELETE maxdata.AAHM_hanging_mplan;
INSERT INTO maxdata.AAHM_hanging_mplan
        SELECT  distinct
        t.planworksheet_id,
        h.merch_lev HM_L, h.merch_id HM_ID, T.TO_MERCH_LEVEL - 10 TO_MERCH_LEVEL ,NULL MERCH_LEVEL,NULL MERCH_ID
        FROM maxdata.AAHN_hanging_nodes h
        JOIN maxdata.planworksheet t
        ON  ((h.MERCH_LEV  >  t.from_merch_level - 10 AND h.MERCH_LEV  < t.to_merch_level - 10)
        OR (h.MERCH_LEV = t.from_merch_level - 10 AND h.MERCH_LEV  < t.to_merch_level - 10 AND h.MERCH_ID = t.FROM_MERCH_ID)) JOIN
        maxdata.mplan m ON m.merch_level = h.merch_lev AND m.merch_id =  h.merch_id AND t.planworksheet_id = m.workplan_id;

I := 2;

WHILE I <= 9 LOOP
        J := I + 1;
        WHILE J <=10 LOOP
                v_sql := 'UPDATE maxdata.AAHM_hanging_mplan H SET MERCH_LEVEL = TO_MERCH_LEVEL, MERCH_ID = (select MAX( lv' || J ||'CTREE_ID)
                        FROM maxdata.LV10CTREE T WHERE T.LV' || I || 'CTREE_ID = H.HM_ID)
                        WHERE H.HM_L = ' || I || ' AND TO_MERCH_LEVEL = ' || J;
--dbms_output.put_line (v_sql);
                EXECUTE IMMEDIATE v_sql;
                J := J + 1;
                n_sqlnum := n_sqlnum + I * 10 +J;
        END LOOP;
        I := I + 1;
END LOOP;

COMMIT;
--exception
                    --WHEN others THEN
                                --NULL;
END;

COMMIT;
n_sqlnum := 100000000;

t_cnt := 1;
BEGIN
WHILE t_cnt <=3  LOOP

SELECT CASE t_cnt when 1 then 'MPLAN_ATTRIB'
                                  when 2 then  'MPLAN_SUBMIT'
                                  ELSE 'MPLAN_WORKING' END
INTO t_mplan_tbl_nm
FROM DUAL;

        v_sql := NULL;
        v_sql2:= NULL;
        DECLARE CURSOR c_mplan_cols IS
        SELECT column_name FROM user_tab_columns
        WHERE table_name = t_mplan_tbl_nm;
        BEGIN
                FOR c1 IN c_mplan_cols LOOP


                        IF v_sql IS NULL  THEN
                                v_sql :='INSERT INTO maxdata.'|| t_mplan_tbl_nm || '( ';
                        ELSE
                                v_sql := v_sql ||',';
                        END IF;
                        v_sql := v_sql || c1.column_name;


                        IF v_sql2 IS NULL  THEN
                                v_sql2 := ' ) SELECT ';
                        ELSE
                                v_sql2 := v_sql2 ||',';
                        END IF;
                        t_col_value :=  CASE c1.column_name
                                        WHEN 'MERCH_LEVEL' THEN 'H.MERCH_LEVEL'
                                        WHEN 'MERCH_ID' THEN 'H.MERCH_ID'
                                        ELSE c1.column_name
                                        END;
                        v_sql2 := v_sql2 || t_col_value;

                END LOOP;
                --exception
                --                  WHEN others THEN
                        --                      NULL;
        END;

        COMMIT;

        n_sqlnum := n_sqlnum + t_cnt;

        EXECUTE IMMEDIATE v_sql || COALESCE(v_sql2,' ') ||
                                ' FROM maxdata.'|| t_mplan_tbl_nm || ' M JOIN maxdata.AAHM_hanging_mplan H ' ||
                                ' ON M.WORKPLAN_ID = H.PLANWORKSHEET_ID ' ||
                                '  AND H.HM_L = M.MERCH_LEVEL AND H.HM_ID = M.MERCH_ID where H.merch_id is not NULL';
        t_cnt := t_cnt + 1;

        COMMIT;
END LOOP;

        --exception
                --    WHEN others THEN
                --      NULL;

RETURN;
END;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                IF v_sql IS NOT NULL THEN
                        t_error_level := 'info';
                        t_sql2 := 'Most recent dynamic SQL. Not necessarily related with the current error';
                        t_sql3 := substr(v_sql,1,255);
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

  GRANT EXECUTE ON "MAXDATA"."P_COMPLETE_MERCH_TREE_LOAD" TO "MADMAX";
