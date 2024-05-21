--------------------------------------------------------
--  DDL for Procedure P_COPY_PW_TMPL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_PW_TMPL" (
        in_unused1                      NUMBER,         -- No more GRIDSETS for PA. Should be always -1.
        in_src_planworksheet_id         NUMBER,         -- Used for FP. PW from which templates copied. and for allocation
        in_unused2                      NUMBER,         -- No more GRIDSETS for PA. Should be always -1.
        in_tar_planworksheet_id         NUMBER,         -- Used for FP. PW from which templates copied. and allocation
        in_prefix_name                  VARCHAR2        -- allocation T_AL(_D)(_CM) or Template: DIMSET (T_TMPL is obsolete)
) AS

/*
------------------------------------------------------------------------------

Change History:
 $Log: 2152_p_copy_pw_tmpl.sql,v $
 Revision 1.11  2007/06/19 14:39:37  clapper
 FIXID AUTOPUSH: SOS 1238247

 Revision 1.7  2006/07/31 15:52:20  saghai
 Ported 23 change

 Revision 1.6  2006/04/14 16:39:29  makirk
 Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

 Revision 1.5  2006/02/17 22:18:54  healja
 Replace $id with $Log
 2152_p_copy_pw_tmpl.sql,v 1.4 2005/12/02 18:38:30 saghai Exp $

Change history:
V6.1
 6.1.0-000 7/14/05    Helmi   Rename maxapp.template to maxdata.dimset_template.
 6.1.0-000 7/13/05    Joseph  Reviewed for 6.1 where T_TMPL is obsolete and no GRIDSETS is used for PA.
                              As this procedure is used by different components (Web and Allocation),
                              we cannot change the interface. So, just nullify the GRIDSETS params.
V5.7
 5.7.0-147 11/01/05   Sachin  #S0327797 Overloaded in_prefix_name parameter for p_copy_move_allocworksht
V5.5
 5.5.0-011 6/16/03    helmi   functionality to distinguish the case of detail--> header alloc copy
 5.5.0-011 3/26/03    helmi   adding special case: allocation(not header record) needs loc_lev_template_id from parent.
 5.5.0-005 3/26/03    helmi   fixing missing time_tmple_id in copied ws.
 5.5.0-000 12/09/02   Sachin  Added n_src_tmpl_id <> 0
 5.5.0-000 11/14/02   helmi   support for allocation tmpl. operations.

V5.4
 5.4.0-018  08/15/02  helmi   fixing the time_template_id update in PWS tbl(has been done earlier on 534)
 5.4.0-000  08/15/02  Sachin  Model not copied
V5.3.3
 07/08/02       Sachin        Update planworksheet_id of template
 11/16/2001 Joseph cho        Add body.
 10/29/2001 Diwakar Raparthi  Initial entry.

------------------------------------------------------------------------------
*/

n_sqlnum                        NUMBER;
v_errmsg                        VARCHAR2(255);
v_sql                           VARCHAR2(5000);
n_src_tmpl_id                   NUMBER;
n_new_tmpl_id                   NUMBER;
n_src_pw_gs_id                  NUMBER;
n_tar_pw_gs_id                  NUMBER;
v_pw_gs                         VARCHAR2(255);
v_pk_col_name                   VARCHAR2(255);
v_template_col_name             VARCHAR2(30);
t_orig_tmpl_type                CHAR(1);
n_loop_num                      NUMBER;
n_merch_template_id             NUMBER;
n_loc_template_id               NUMBER;
n_time_template_id              NUMBER;
n_loc_lev_template_id           NUMBER;
n_dst_loc_template_id           NUMBER;
n_base_merch_template_id        NUMBER;

BEGIN
n_sqlnum := 1000;

-- Log the parameters of the proc.

v_sql := 'Prm:' ||
                in_unused1     || ',' ||
                in_src_planworksheet_id  || ',' ||
                in_unused2     || ',' ||
                in_tar_planworksheet_id || ',' ||
                in_prefix_name  ;
maxdata.ins_import_log ('p_copy_pw_tmpl','info', v_sql, NULL, NULL, NULL);
--commit;

n_sqlnum := 2000;

IF in_unused1 <> -1 OR in_unused2 <> -1 THEN
                raise_application_error (-20001,'Unsupported parameters');
END IF;

IF in_src_planworksheet_id = -1 OR in_tar_planworksheet_id = -1 THEN
                raise_application_error (-20001,'Source or target id not supplied');
END IF;

IF in_prefix_name LIKE 'T_AL%' THEN
        n_loop_num := 6; -- for allocation we might need to loop 6 times.
        n_src_pw_gs_id := in_src_planworksheet_id;
        n_tar_pw_gs_id := in_tar_planworksheet_id;
        v_pw_gs := 'maxdata.alloc_def';
        v_pk_col_name := 'alloc_def_id';

        -- we need to get template_id for that exists in all this Col.
        SELECT  merch_template_id , loc_template_id, loc_lev_template_id,
                dst_loc_template_id, base_merch_template_id, time_template_id
        INTO    n_merch_template_id, n_loc_template_id, n_loc_lev_template_id,
                n_dst_loc_template_id, n_base_merch_template_id, n_time_template_id
        FROM maxdata.alloc_def
        WHERE alloc_def_id = in_src_planworksheet_id;
ELSE
        n_loop_num := 3; -- for planning and PA.
        n_src_pw_gs_id := in_src_planworksheet_id;
        n_tar_pw_gs_id := in_tar_planworksheet_id;
        v_pw_gs := 'maxdata.planworksheet';
        v_pk_col_name := 'planworksheet_id';

        v_sql :='SELECT merch_template_id, loc_template_id, time_template_id ' ||
                ' FROM ' || v_pw_gs ||
                ' WHERE ' || v_pk_col_name || ' = :n_src_pw_gs_id';
        EXECUTE IMMEDIATE v_sql
        INTO n_merch_template_id, n_loc_template_id,n_time_template_id
        USING n_src_pw_gs_id;
END IF;

-- special case: the record is from allocation and it is not a header.
IF in_prefix_name IN ('T_AL_D','T_AL_D_CM') THEN
        SELECT loc_lev_template_id INTO n_loc_lev_template_id
        FROM maxdata.alloc_def
        WHERE alloc_def_id  = (SELECT parent_alloc_id
                               FROM maxdata.alloc_def
                               WHERE alloc_def_id = in_src_planworksheet_id);
END IF;

-- Special case, when called from p_copy_move_allocworksht procedure
-- If in_copydata_flg = 1, then we need to copy only time_template,loc_lev_template,dst_loc_template and base_merch_template

IF in_prefix_name IN ('T_AL_D_CM','T_AL_CM') THEN
        n_merch_template_id     := NULL;
        n_loc_template_id       := NULL;
--      n_time_template_id    DO NOT CHANGE
--      n_loc_lev_template_id DO NOT CHANGE
--      n_dst_loc_template_id DO NOT CHANGE
--      n_base_merch_template_id  DO NOT CHANGE

END IF;


FOR n1 IN 1..n_loop_num LOOP
        n_sqlnum := 2000 + n1;

        IF n1 = 1 THEN
                v_template_col_name := 'loc_template_id';
                n_src_tmpl_id := n_loc_template_id;
        ELSIF n1 = 2 THEN
                v_template_col_name := 'merch_template_id';
                n_src_tmpl_id := n_merch_template_id;
        ELSIF n1 = 3 THEN
                v_template_col_name := 'time_template_id';
                n_src_tmpl_id := n_time_template_id;
        ELSIF n1 = 4 THEN
                v_template_col_name := 'loc_lev_template_id';
                n_src_tmpl_id := n_loc_lev_template_id;
        ELSIF n1 = 5 THEN
                v_template_col_name := 'base_merch_template_id';
                n_src_tmpl_id := n_base_merch_template_id;
        ELSE -- 6
                v_template_col_name := 'dst_loc_template_id';
                n_src_tmpl_id := n_dst_loc_template_id;
        END IF;


        IF n_src_tmpl_id IS NOT NULL AND n_src_tmpl_id <> 0 THEN
            -- Check the template type. For Model type, we don't copy the template but
            -- just set the pointer to it. Model is shared by many worksheets.
                n_sqlnum := 2100 + n1;

                SELECT template_type INTO t_orig_tmpl_type
                FROM maxdata.dimset_template
                WHERE template_id = n_src_tmpl_id;

                IF t_orig_tmpl_type = 'M' THEN
                BEGIN
                        -- Set the pointer to Model template.
                        n_sqlnum := 2400 + n1;
                        v_sql :='update ' || v_pw_gs ||
                                ' set ' || v_template_col_name || '= :n_src_tmpl_id' ||
                                ' where ' || v_pk_col_name || ' = :n_tar_pw_gs_id';

                        EXECUTE IMMEDIATE v_sql USING n_src_tmpl_id, n_tar_pw_gs_id;
                END;
                ELSE
                BEGIN
                        -- Copy the template.

                        n_sqlnum := 2500 + n1;
                        maxdata.p_copy_tmpl_one (
                                'A',
                                n_src_tmpl_id,
                                'A',
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                NULL,
                                n_new_tmpl_id
                                );
                        COMMIT;

                        -- Make the target pw/gs point to the new template.

                        n_sqlnum := 2800 + n1;
                        v_sql := 'update ' || v_pw_gs ||
                                 ' set ' || v_template_col_name || '= :n_new_tmpl_id' ||
                                 ' where ' || v_pk_col_name || ' = :n_tar_pw_gs_id';

                        EXECUTE IMMEDIATE V_SQL USING N_NEW_TMPL_ID, N_TAR_PW_GS_ID;

                        -- Update planworksheet_id of template so that DELETE CASCADE
                        -- may delete from TEMPLATE when a worksheet is deleted.
                        -- GRIDSETS or ALLOC_DEF works differently. There is NO FK from
                        -- TEMPLATE to GRIDSETS/ALLOC_DEF. Deletion is handled by the app for
                        -- GRIDSETS or by p_delete_allocwrksht for ALLOC_DEF (ALLOC_DEF_NEW).
                        -- In v6.1, GRIDSETS PA data were all moved to PLANWORKSEET.

                        IF in_prefix_name = 'DIMSET' THEN
                                n_sqlnum := 2900 + n1;
                                v_sql :='update maxdata.dimset_template '||
                                        ' set planworksheet_id = :n_tar_pw_gs_id '||
                                        ' where template_id = :n_new_tmpl_id';

                                EXECUTE IMMEDIATE v_sql USING n_tar_pw_gs_id, n_new_tmpl_id;
                        END IF;

                        COMMIT;
                END;
                END IF; -- if 'M'
        END IF; -- if id not null
END LOOP;

EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;

                v_sql := SQLERRM ||
                        '(p_copy_pw_tmpl (' ||
                        in_unused1     || ',' ||
                        in_src_planworksheet_id  || ',' ||
                        in_unused1     || ',' ||
                        in_tar_planworksheet_id || ',' ||
                        in_prefix_name ||
                        ', SQL#:' || n_sqlnum || ')';
                maxdata.ins_import_log ('p_copy_pw_tmpl','error', v_sql, NULL, NULL, NULL);
                COMMIT;

                raise_application_error (-20001,v_sql);

END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_PW_TMPL" TO "MADMAX";
