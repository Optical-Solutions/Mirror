--------------------------------------------------------
--  DDL for Procedure P_COMPLETE_MERCH_TREE_APP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COMPLETE_MERCH_TREE_APP" (
        in_lvxctree_lvl         NUMBER,   -- m_lev where to add the member (cmast level). Must be 2 <= and <= 9
        in_lvxctree_par_id      NUMBER,   -- the parent id from the corresponding ctree table NOT NULL
        in_lvxcmast_name        VARCHAR2, -- name of the new member (if NULL then we generate name in the proc).
        in_mem_cnt              NUMBER,   -- number of members to add
        in_future_prm2          NUMBER,   -- (-1)
        in_future_prm3          NUMBER,   -- (-1)
        out_lv10mast_id OUT     NUMBER
) AS

/*
----------------------------------------------------------------------
$Log: 2359_p_complete_merch_tree_app.sql,v $
Revision 1.9  2007/06/19 14:38:56  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.3  2006/06/07 22:40:57  saghai
removed hash sign


Revision 1.1  2006/06/05 16:06:26  anchan
Renumbered the numeric prefix of the script.
Added for rename from 2360_p_complete_merch_tree_app.sql.
See originally named file for history prior to the rename.

Revision 1.7  2006/05/31 17:56:40  saghai
S0351861 -  Handle the apostrophe in the name

Revision 1.6  2006/04/14 16:39:34  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

Revision 1.5  2006/01/06 15:05:30  healja
adding functionalities to generate unique names in the procedure in case the give name is NULL.
Review Diwakar

Revision 1.4  2005/12/14 19:05:18  healja
changing data length for proc_name ..

Revision 1.3  2005/11/02 20:57:38  healja
session table name change.
fixing little bug for lv1 DSQL change from cmast to ctree.
Review Mark

Revision 1.2  2005/10/17 20:52:49  healja
getting an out param for the application and all changes required for it


Description:

Complete the merch tree from the given level (in_lvxctree_lvl) under the
given parent (in_lvxctree_par_id) all the way down to the bottom
using the cmast names in HIER_LEVEL table. On each level, add
the given number of members (in_mem_cnt)
Will return the lv10mast_id of the first inserted SKU item to the application.

Usage:

Called by the app, so coordinate any interface changes with the app.
-------------------------------------------------------------------------
*/

n_sqlnum                NUMBER(10)         := 1000;
t_proc_name             VARCHAR2(64)       := 'p_complete_merch_tree_app';
t_error_level           VARCHAR2(6)        :=  'info';
t_call                  VARCHAR2(1000);
t_errmsg                VARCHAR2(255)      := NULL;
t_errorcode             NUMBER(10)         := 0;
v_sql                   VARCHAR2(4000)     := '';
t_sql2                  VARCHAR2(255);
t_sql3                  VARCHAR2(255);
t_cnt                   NUMBER(10);
t_lv10mast_id           NUMBER(10);
t_lvxcmast_name         VARCHAR2(100);


BEGIN

-- Log the parameters of the procedure
n_sqlnum := 200;
t_call := t_proc_name || '( ' ||
        COALESCE(in_lvxctree_lvl, -123)  || ',' ||
        COALESCE(in_lvxctree_par_id, -123)  || ',' ||
        COALESCE(in_lvxcmast_name, 'NULL')  ||', ' ||
        COALESCE(in_mem_cnt, -123)   || ',' ||
        COALESCE(in_future_prm2, -123)  || ',' ||
        COALESCE(in_future_prm3, -123)  || ',' ||
        'OUT )';

--maxdata.ins_import_log  (t_proc_name, t_error_level, t_call, v_sql, n_sqlnum, NULL);
--COMMIT;

n_sqlnum := 500;

DELETE FROM maxdata.sess_complete_merch_tree;

n_sqlnum := 600;
t_cnt := 1;

-- Reserve lv10mast_ids as necessary . wee need that to return contigues lv10mast_id to the apps.
maxapp.p_get_next_key (10, 1, in_mem_cnt, t_lv10mast_id, t_errmsg);
COMMIT;

out_lv10mast_id := t_lv10mast_id;

WHILE t_cnt <= in_mem_cnt LOOP
        n_sqlnum := n_sqlnum + t_cnt;

        INSERT INTO maxdata.sess_complete_merch_tree
        SELECT level_id - 10 m_lev, REPLACE(level_name,'''','''''') m_name, NULL lvxcmast_id, add_unique
        FROM maxdata.hier_level
        WHERE hier_id = 11; -- cmast hier_id

        SELECT COALESCE (REPLACE(in_lvxcmast_name,'''',''''''), m_name || '-SG-' ||CAST (t_lv10mast_id AS VARCHAR2(10)))
        INTO t_lvxcmast_name
        FROM maxdata.sess_complete_merch_tree
        WHERE m_lev = in_lvxctree_lvl;

        maxdata.p_complete_merch_tree (
                in_lvxctree_lvl,
                in_lvxctree_par_id,
                t_lvxcmast_name,
                t_cnt,
                t_lv10mast_id,
                in_future_prm2,
                in_future_prm3);

        t_cnt :=  t_cnt + 1;
        t_lv10mast_id := t_lv10mast_id + 1;

        DELETE FROM maxdata.sess_complete_merch_tree;
END LOOP;

COMMIT;
RETURN;

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

                --maxdata.ins_import_log (t_proc_name, t_error_level, v_sql, null, n_sqlnum, NULL);
                COMMIT;

                RAISE_APPLICATION_ERROR(-20001,v_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COMPLETE_MERCH_TREE_APP" TO "MADMAX";
