--------------------------------------------------------
--  DDL for Procedure P_COPY_TMPL_ONE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_COPY_TMPL_ONE" (
                in_src_type                     VARCHAR2 ,   -- ?A? or ?M? or ?T?
                in_src_template_id              NUMBER,
                in_tar_type                     VARCHAR2,    -- ?A? or ?M? or ?T?
                in_tar_template_id              NUMBER,      -- Used by P_SAVE_TMPL to replace original with tmp.
                in_dy_name_id                   NUMBER,      -- Used for temp table name.
                in_new_name                     VARCHAR2,    -- (225), New template name. Required only for M->M copy
                in_new_max_user_id              NUMBER  ,    -- Only for M->M copy
                in_new_max_group_id             NUMBER ,     -- Only for M->M copy
                in_prefix_name                  VARCHAR2,    --'T_AL' or 'T_TMPL'
                out_template_id      OUT        NUMBER       -- 1 if given name is duplicate
) AS

/*
-----------------------------------------------------------------------
MODIFICATION HISTORY

$Log: 2150_p_copy_tmpl_one.sql,v $
Revision 1.15  2007/06/19 14:39:39  clapper
FIXID AUTOPUSH: SOS 1238247

Revision 1.10  2006/06/06 22:44:26  saghai
61 or 22 should not reference maxapp.template but the the new maxdata.dimset_template.

Revision 1.9  2006/04/14 16:39:28  makirk
Removed import_log delete code (redundant), added change history logging where needed, commented out param logging commit

Revision 1.8  2006/03/17 14:18:30  joscho
Added new column visible_prop_flg

Revision 1.7  2006/03/14 16:16:41  joscho
Revert back to 5.6 in order to support Allocation 2.2.
Only minor changes were added to support 6.1.



V5.6
 5.6.0-000 10/14/03   Sachin  Added columns to template_member for mlt
 V5.5
 5.5.0-000 4/30/03    helmi   allowing copy M to A tmpl.
 5.5.0-000 11/12/02   helmi   Increase the length of in_new_name to 80 char.
 5.5.0-000 11/12/02   helmi   fixing the call or the proc to itself(in_prefix_name).
 5.5.0-all 11/12/02   helmi   support for allocation tmpl. operations.

V5.4
 5.4.0-031 11/26/02   Sachin  Added code to handle A--> A case for Visible partial flag.
 5.4.0-028 11/8/02    Sachin  Modified decode stmt.
 5.4.0-028 11/8/02    Sachin  Hide Time Member Changes for Rama
 5.4.0-000 8/21/02    Joseph  Set master_set_flag. List allowed src/target types.
 5.4.0-000 8/15/02    Sachin  Allow to copy T -> M. Target tmpl id required only for T->A,M.
 5.4.0-000 8/7/02     helmi   Allow A->A
 5.4.0-000 8/2/02     Jcho    Allow M->A requested by p_copy_pw_tmpl.
 5.4.0-000 7/31/02    helmi   adding multiple cols to the insert stmt of the tmpl, tmpl_lev and tmpl_mem

V5.3.2
 Rg   4/10/02         commented out code so that from_id is copied from source to target. for sql, its
                      fixed in 531\41_05.
V5.3.1
 Joseph Cho   03/19/2002      For save (T->A), replace rows in temp template
                              with ones with permanent template_id.

V5.3
 Joseph Cho   02/19/2002      Remove unnecessary bind variables.
 Joseph Cho   02/18/2002      Copy member_count.
 Diwakar Raparthi     12/22/2001     Added Condition to copy from_id as template_id from source to target for time
                                     templates when not active to active, otherwise copy from_id as source to target
 Joseph Cho           12/14/2001     Copy start/end_date, filterset_id
 Diwakar Raparthi     10/29/2001     Initial Entry
 ---------   ------  -------------------------------------------

 DESCRIPTION

 USED BY THE APP, SO ITS INTERFACE CHANGE SHOULD BE COORDINATED WITH THE APP

 This procedure is used to copy a dimension set template.
 See its spec (in CVS) for more details
------------------------------------------------------------------------
*/

v_suffix                VARCHAR2(60);
v_src_tmpl              VARCHAR2(60);
v_src_lev               VARCHAR2(60);
v_src_mem               VARCHAR2(60);
v_tar_tmpl              VARCHAR2(60);
v_tar_lev               VARCHAR2(60);
v_tar_mem               VARCHAR2(60);
v_errmsg                VARCHAR2(255);
v_sql                   VARCHAR2(8000);
v_orig_type             CHAR(1);
n_sqlnum                NUMBER;
t_sql                   VARCHAR2(255);
n_name                  VARCHAR2(225);
n_user_id               NUMBER;
n_group_id              NUMBER;
n_from_id               NUMBER;
n_dimension_type        NUMBER;
n_template_id           NUMBER;
n_from_tmpl_id          NUMBER;
n_master_set_flag       NUMBER;

BEGIN
n_sqlnum := 1000;
-- Log the parameters of the proc.

t_sql := 'Prm:' ||
         'p_copy_tmpl_one (' ||
         in_src_type  || ',' ||
         in_src_template_id || ',' ||
         in_tar_type  || ',' ||
         in_tar_template_id || ',' ||
         in_dy_name_id  || ',' ||
         in_new_name           || ',' ||
         in_new_max_user_id    || ',' ||
         in_new_max_group_id   || ',' ||
         in_prefix_name               || ',' ||
         'out_template_id'  || ')';
maxdata.ins_import_log ('p_copy_tmpl_one','info', t_sql, NULL, NULL, NULL);
--COMMIT;

-- Check allowed SRC/TARGET types.

IF NOT (
     (in_src_type = 'A' AND in_tar_type = 'A') OR -- p_copy_pw_tmpl
     (in_src_type = 'A' AND in_tar_type = 'M') OR -- application does A->M.
     (in_src_type = 'A' AND in_tar_type = 'T') OR -- p_open
     (in_src_type = 'M' AND in_tar_type = 'A') OR -- allowed (from application)
     (in_src_type = 'M' AND in_tar_type = 'M') OR -- copy M to M
     (in_src_type = 'M' AND in_tar_type = 'T') OR -- p_open PW with master dimension set
     (in_src_type = 'T' AND in_tar_type = 'A') OR -- p_save
     (in_src_type = 'T' AND in_tar_type = 'M')    -- p_save PW with master dimension set
     --(in_src_type = 'T' AND in_tar_type = 'T')  -- not allowed
     )  THEN
        RAISE_APPLICATION_ERROR (-20001, 'Not allowed copy types');
END IF;


IF in_src_template_id IS NULL THEN
        RAISE_APPLICATION_ERROR ( -20001, ' Source Template ID Cannot be null');
END IF;

IF in_src_type = 'T' OR in_tar_type = 'T' THEN
        IF in_dy_name_id IS NULL  THEN
                RAISE_APPLICATION_ERROR (-20001, 'If source/target type is T, then PW ID for temp table name required');
        END IF;
ELSE
        IF in_dy_name_id IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'If source/target type is not T, then PW ID for temp table name must be null');
        END IF;
END IF;

IF in_src_type in ('A', 'M') AND in_tar_type = 'T' THEN
        IF in_tar_template_id IS NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'If copying peramanent to template, target template id required');
        END IF;
ELSIF NOT (in_src_type = 'T' AND (in_tar_type in ('A', 'M'))) THEN
        IF in_tar_template_id IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'Target template id must be specified only for SAVE operation');
        END IF;
END IF;

-- If M->M copy, then three more parameters are used.

IF in_src_type = 'M' AND in_tar_type= 'M' THEN
        IF in_new_name IS NULL THEN
                RAISE_APPLICATION_ERROR (-20001,'New name required for copying model to model');
        END IF;
ELSE
        IF in_new_name IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001,'New name required only for copying model');
        END IF;
END IF;

n_sqlnum := 2000;
v_suffix  :=  in_dy_name_id;

IF in_src_type = 'T' THEN
        v_src_tmpl := 'MAXDATA.'|| in_prefix_name || v_suffix;
        v_src_lev  := 'MAXDATA.'|| in_prefix_name || '_LEV' || v_suffix;
        v_src_mem  := 'MAXDATA.'|| in_prefix_name || '_MEM' || v_suffix;
ELSE
        v_src_tmpl := 'MAXDATA.DIMSET_TEMPLATE';
        v_src_lev  := 'MAXDATA.DIMSET_TEMPLATE_LEV';
        v_src_mem  := 'MAXDATA.DIMSET_TEMPLATE_MEM';
END IF;

n_sqlnum := 3000;

IF in_tar_type = 'T' THEN
        v_tar_tmpl := 'MAXDATA.'|| in_prefix_name || v_suffix;
        v_tar_lev  := 'MAXDATA.'|| in_prefix_name || '_LEV' || v_suffix;
        v_tar_mem  := 'MAXDATA.'|| in_prefix_name || '_MEM' || v_suffix;
ELSE
        v_tar_tmpl := 'MAXDATA.DIMSET_TEMPLATE';
        v_tar_lev  := 'MAXDATA.DIMSET_TEMPLATE_LEV';
        v_tar_mem  := 'MAXDATA.DIMSET_TEMPLATE_MEM';
END IF;

IF in_tar_template_id IS NULL THEN
        MAXAPP.P_GET_NEXT_KEY(1200,0,1,out_template_id, v_errmsg);
        IF v_errmsg IS NOT NULL THEN
                RAISE_APPLICATION_ERROR (-20001, 'Error While getting new template id ' || v_errmsg);
        END IF;
        COMMIT; -- commit the new temp id generation.
ELSE
        out_template_id := in_tar_template_id;
END IF;

-- Fetch the source template and check if the type matches.

n_sqlnum := 4000;
v_sql := 'Select template_type, name, max_user_id, max_group_id, ' ||
        'from_id, dimension_type ' ||
        ' From ' || v_src_tmpl ||
        ' Where template_id = :in_src_template_id';

EXECUTE IMMEDIATE v_sql
INTO v_orig_type, n_name, n_user_id, n_group_id,
     n_from_id, n_dimension_type
USING in_src_template_id;

--if n_dimension_type = '3'  and not (in_src_type = 'A' and in_tar_type = 'A') then
--      n_from_id := out_template_id;
--end if;

-- Check if the supplied source type is the same as in the source template.
-- Exception: If copy is done for copying a worksheet which uses Master Template,
-- then orig_type is 'M' though p_copy_pw_tmpl passes 'A' as source type.

IF (v_orig_type <> in_src_type) AND NOT (v_orig_type = 'M' AND in_src_type = 'A') THEN
        RAISE_APPLICATION_ERROR (-20001, 'Template type Different from expected type ');
END IF;

--- Copy Template with 'I' Type showing that we are in progress of copying
-- The above comment is not true anymore. We save target type directly.

-- If M->M, then use the supplied name.

IF in_src_type = 'M' AND in_tar_type='M' THEN
        N_name := in_new_name;
        N_user_id := in_new_max_user_id;
        N_group_id := in_new_max_group_id;
END IF;

-- Copy the template. Copying template/levels/members are done using
-- a single transaction.

-- For SS, start transaction.   Oracle: implicit transaction.
-- begin tran
-- n_tran_started=1

-- Pre-v61:
-- When we assign a Model to a planworksheet, save the
-- original (model) template id to the active template. Copying M to A is done by two steps:
-- copy from M to T (when open worksheet), and then copy T to A (when save).
-- When we OPEN or SAVE a worksheet or copy template as part of a worksheet,
-- then we carry along from_template id.
-- Post-v61:
-- Directly copy from M to A.

n_from_tmpl_id := NULL;
IF (in_src_type = 'M' AND in_tar_type = 'T') OR         -- pre-v61
   (in_src_type = 'M' AND in_tar_type = 'A') THEN       -- post-v61
        n_from_tmpl_id := in_src_template_id;
ELSIF (in_src_type = 'T' AND in_tar_type = 'A') OR
        (in_src_type = 'A' AND in_tar_type = 'A') OR
        (in_src_type = 'A' AND in_tar_type = 'T') THEN

        n_sqlnum := 5000;
        v_sql := 'select from_template from '|| v_src_tmpl ||
                ' where template_id = :in_src_template_id';

        EXECUTE IMMEDIATE v_sql
        INTO n_from_tmpl_id
        USING in_src_template_id;
END IF;

-- Copy over Master_set_flag when we open or save Master Set template.

n_master_set_flag := 0;
IF (in_src_type = 'M' AND in_tar_type = 'T') OR
   (in_src_type = 'T' AND in_tar_type = 'M') OR
   (in_src_type = 'M' AND in_tar_type = 'M') THEN       -- post-v61

        v_sql := 'select master_set_flag from '|| v_src_tmpl ||
                ' where template_id = :in_src_template_id';

        EXECUTE IMMEDIATE v_sql
        INTO n_master_set_flag
        USING in_src_template_id;
END IF;

-- copy the template header record.

v_sql := 'INSERT INTO ' || v_tar_tmpl ||
          ' ( template_id, '    ||
             'dimension_type, ' ||
             'name ,'           ||
             'from_id, '        ||
             'from_level, '     ||
             'to_level, '       ||
             'max_user_id, '    ||
             'max_group_id, '   ||
             'path_id, '        ||
             'template_type, '  ||
             'start_date, '     ||
             'end_date, '       ||
             'filterset_id, '   ||
             'member_count, '   ||
             'recalc_option, '  ||
             'from_template, '  ||
             'master_set_flag, '||
             'set_type, '       ||
             'dc_id, '          ||
             'num_periods, '    ||
             'dir_period_flg, ' ||
             'inc_curr_period_flg, ' ||
             'visible_prop_flg )' ||

          ' SELECT ' ||
             ':out_template_id, ' ||
             'dimension_type, ' ||
             ':name, '          ||
             ':from_id, '       ||
             'from_level, '     ||
             'to_level, '       ||
             ':max_user_id, '   ||
             ':max_group_id, '  ||
             'path_id, '        ||
             ':in_tar_type, '   ||
             'start_date, '     ||
             'end_date, '       ||
             'filterset_id, '   ||
             'member_count, '   ||
             'recalc_option, '  ||
             ':n_from_tmpl_id, ' ||
             ':n_master_set_flag, ' ||
             'set_type, '       ||
             'dc_id, '          ||
             'num_periods, '    ||
             'dir_period_flg, ' ||
             'inc_curr_period_flg, ' ||
             'visible_prop_flg ' ||
           ' FROM ' || v_src_tmpl ||
           ' WHERE template_id = :in_src_template_id';

n_sqlnum := 7000;

--dbms_output.put_line(substr(v_sql,1,255));
EXECUTE IMMEDIATE v_sql
USING out_template_id, n_name, n_from_id,
      n_user_id, n_group_id, in_tar_type, n_from_tmpl_id,
      n_master_set_flag, in_src_template_id;
--commit;

--- Copy all levels
n_sqlnum := 8000;

v_sql := 'INSERT INTO ' || v_tar_lev ||
            '( template_id, ' ||
              'level_number, ' ||
              'level_seq, ' ||
              'level_name, ' ||
              'dynamic_flag,' ||
              'kpi_field_id,' ||
              'kpi_field_level,' ||
              'method_type,' ||
              'no_of_groups,' ||
              'level_incl_flag,' ||
              'partial_flag , ' ||
              'autocreate_flag, '||
              'visible_partial_flg) ' ||
         'SELECT ' ||
            ' :out_template_id,' ||
            ' level_number ,' ||
            ' level_seq, ' ||
            ' level_name, ' ||
            ' dynamic_flag, ' ||
            ' kpi_field_id, ' ||
            ' kpi_field_level, ' ||
            ' method_type, ' ||
            ' no_of_groups, ' ||
            ' level_incl_flag, ' ||
            ' partial_flag, ' ||
            ' autocreate_flag, ' ||
            ' DECODE(:in_src_type ||:in_tar_type,''AT'',visible_partial_flg,''TA'',visible_partial_flg,''AA'',visible_partial_flg,0) '||
        ' FROM ' || v_src_lev  ||
        ' WHERE  template_id = :in_src_template_id';

n_sqlnum := 9000;

EXECUTE IMMEDIATE v_sql
USING out_template_id,
      in_src_type, in_tar_type,
      in_src_template_id;
--commit;

--- copy all members

v_sql := 'INSERT INTO ' || v_tar_mem ||
           ' ( template_id, ' ||
           ' level_number, ' ||
           ' member_id, ' ||
           ' member_name, ' ||
           ' parent_member_id, '  ||
           ' exclude_flag, ' ||
           ' partial_flag , ' ||
           ' removed_Flag , ' ||
           ' days_in_period1 , ' ||
           ' visible_flg )' ||
        ' SELECT ' ||
            ':out_template_id, ' ||
            'level_number, ' ||
            'member_id, ' ||
            'member_name, ' ||
            'parent_member_id, ' ||
            'exclude_flag, ' ||
            'partial_flag , '||
            'removed_Flag , ' ||
           ' days_in_period1 , ' ||
           ' visible_flg ' ||
        ' FROM ' || v_src_mem ||
        ' WHERE template_id = :in_src_template_id';

n_sqlnum := 10000;

EXECUTE IMMEDIATE v_sql
USING out_template_id, in_src_template_id;

-- If we are copying from 'T' to 'A' and its template id is not a permanent one,
-- then replace old rows with the ones that have the permanent ids.
-- Updating the template id doesn't work because of FK from child table.

IF in_tar_template_id IS NULL AND (in_src_type = 'T' AND in_tar_type = 'A') THEN
        n_sqlnum := 6000;
        v_sql := 'DELETE FROM '|| v_src_mem ||
                ' Where template_id = :in_src_template_id';
        EXECUTE IMMEDIATE v_sql
        USING in_src_template_id;

        v_sql := 'DELETE FROM '|| v_src_lev ||
                ' Where template_id = :in_src_template_id';
        EXECUTE IMMEDIATE v_sql
        USING in_src_template_id;

        v_sql := 'DELETE FROM '|| v_src_tmpl ||
                ' Where template_id = :in_src_template_id';
        EXECUTE IMMEDIATE v_sql
        USING in_src_template_id;

        -- Copy the permanent ones to the temporary ones.
        -- NOTE that it is a recursive call, but it's ok because it is copying
        -- 'A' to 'T'.

        n_sqlnum := 6500;
        maxdata.p_copy_tmpl_one('A', out_template_id, 'T', out_template_id,
                                in_dy_name_id, NULL, NULL, NULL, in_prefix_name, n_template_id);
END IF;



/** This is not necessary anymore. We store the target type directly.
-- Put the target type in order to indicate that copy was complete.

n_sqlnum := 11000;
v_sql := ' UPDATE ' || v_tar_tmpl ||
        ' SET template_type = :in_tar_type '||
        ' WHERE template_id = :out_template_id';
EXECUTE IMMEDIATE v_sql
using in_tar_type, out_template_id;
  **/

COMMIT;
--SS: commit tran
--SS: n_tran_started=0


EXCEPTION
        WHEN OTHERS THEN
                ROLLBACK;
                --SS: rollback tran if n_tran_started=1

                t_sql := SQLERRM ||
                        ' (p_copy_tmpl_one (' ||
                        in_src_type         || ',' ||
                        in_src_template_id  || ',' ||
                        in_tar_type         || ',' ||
                        in_tar_template_id  || ',' ||
                        in_dy_name_id       || ',' ||
                        in_new_name         || ',' ||
                        in_new_max_user_id  || ',' ||
                        in_new_max_group_id || ',' ||
                        in_prefix_name      || ',' ||
                        'out_template_id'   || ')' ||
                        ', SQL#:' || n_sqlnum || ')';
                maxdata.ins_import_log ('p_copy_tmpl_one','info', t_sql, NULL, NULL, NULL);
                COMMIT;

                raise_application_error (-20001,t_sql);
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_COPY_TMPL_ONE" TO "MAXAPP";
  GRANT EXECUTE ON "MAXDATA"."P_COPY_TMPL_ONE" TO "MADMAX";
