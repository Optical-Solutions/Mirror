--------------------------------------------------------
--  DDL for Procedure P_NO_MVMT_FLAG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_NO_MVMT_FLAG" 
AS
  cursor c_bigjoin is
    select lv10loc.lv10loc_id, lv10loc.lv10mast_id, pogmaster.pog_model_id,
           pogmaster.pog_lv4loc_id
    from   maxdata.lv10loc, maxdata.pogmaster
    where  lv10loc.lv7loc_id = pogmaster.pog_model_id and
           pogmaster.current_pog = 1 and
           pogmaster.live_lv7loc_id in
           (select lv7loc_id
            from   maxdata.lv7loc, maxdata.lv4loc
            where  lv7loc.lv4loc_id = lv4loc.lv4loc_id and
                   lv4loc.num_user1 = 1 and
                   lv7loc.lv7mast_id = (select lv7mast_id
                                        from   maxdata.lv7mast
                                        where  lv7mast_userid = 'DEFAULT')
           );

  t_lv10loc_id         integer ;
  t_lv10mast_id        integer ;
  t_pog_model_id       integer ;
  t_pog_lv4loc_id      integer ;

  t_positions          INTEGER ;

begin


 open c_bigjoin ;
  loop
    fetch  c_bigjoin into t_lv10loc_id, t_lv10mast_id, t_pog_model_id, t_pog_lv4loc_id ;
    exit when c_bigjoin%notfound;

    BEGIN
    t_positions := 0 ;

    select positions
           into   t_positions
           from   maxdata.lv10positions
           where  lv4loc_id = t_pog_lv4loc_id and
                  lv10mast_id = t_lv10mast_id ;

      if t_positions > 1 then
          update maxdata.lv10loc
          set    no_mvmt_flag = 1
          where  lv10loc_id = t_lv10loc_id ;

          update maxdata.lv10positions
          set    positions = positions - 1
          where  lv4loc_id = t_pog_lv4loc_id and
                 lv10mast_id = t_lv10mast_id ;

       end if ;

    EXCEPTION
       WHEN NO_DATA_FOUND then
          maxdata.ins_import_log('P_NO_MVMT_FLAG','INFORMATION','Store '||t_pog_lv4loc_id||
          ' LV10MAST Id '||t_lv10mast_id||' not found in LV10POSITIONS',null,null,null) ;

   END;
  end loop ;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_NO_MVMT_FLAG" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_NO_MVMT_FLAG" TO "MAXUSER";
