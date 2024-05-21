--------------------------------------------------------
--  DDL for Procedure P_RECALC_SPACE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_RECALC_SPACE" 
as
  commit_ctr      integer ;
  process_ctr     integer ;

-- modified on 27-MAR-98
-- cursor processing since this process would update large # of records

begin
  maxdata.ins_import_log('P_RECALC_SPACE','INFO','Started ...',null,null,null) ;
  commit ;

  commit_ctr := 0 ;
  process_ctr := 0 ;
  for c1 in ( select lv7loc_id
              from   maxdata.lv7loc
              where  recalc_flag = 2 ) loop

    update maxdata.lv7loc
       set recalc_flag = 1
     where lv7loc_id = c1.lv7loc_id ;
    commit_ctr := commit_ctr + 1 ;
    process_ctr := process_ctr + 1 ;

    if commit_ctr > 50 then
      maxdata.ins_import_log('P_RECALC_SPACE','INFO',
        'Level 7 Loc 50 committed total '||process_ctr,null,null,null) ;
      commit ;
      commit_ctr := 0 ;
    end if ;
  end loop ;
  maxdata.ins_import_log('P_RECALC_SPACE','INFO',
    'Level 7 Loc completed total '||process_ctr,null,null,null) ;
  commit ;

  commit_ctr := 0 ;
  process_ctr := 0 ;
  for c1 in ( select lv5loc_id
              from   maxdata.lv5loc
              where  recalc_flag = 2 ) loop

    update maxdata.lv5loc
       set recalc_flag = 1
     where lv5loc_id = c1.lv5loc_id ;
    commit_ctr := commit_ctr + 1 ;
    process_ctr := process_ctr + 1 ;

    if commit_ctr > 50 then
      maxdata.ins_import_log('P_RECALC_SPACE','INFO',
        'Level 5 Loc 50 committed total '||process_ctr,null,null,null) ;
      commit ;
      commit_ctr := 0 ;
    end if ;
  end loop ;
  maxdata.ins_import_log('P_RECALC_SPACE','INFO',
    'Level 5 Loc completed total '||process_ctr,null,null,null) ;
  commit ;

  maxdata.ins_import_log('P_RECALC_SPACE','INFO','Completed ...',null,null,null) ;
  commit ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_RECALC_SPACE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_RECALC_SPACE" TO "MAXUSER";
