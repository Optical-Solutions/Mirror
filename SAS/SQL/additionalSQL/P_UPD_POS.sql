--------------------------------------------------------
--  DDL for Procedure P_UPD_POS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_POS" ( p_cmast_lvl IN integer)
AS

    step integer ;
cursor c_temp_pos is
     select lv4loc_id,lv10mast_id,sum(ins_del)
       from maxdata.temp_pos
     group by lv4loc_id,lv10mast_id ;

cursor c_lastone is
    select store_loc_id, lv10mast_id, sum(nvl(positions,0))
    from   maxdata.lv10mvmt
    group  by store_loc_id, lv10mast_id
    having sum(nvl(positions,0)) = 0 ;

   t_store_loc_id   integer ;
   t_sum_pos        numeric ;
   t_lv10mast_id   integer ;
   t_char_user9    varchar(80) ;
   t_lv7loc_id     integer ;
   t_lv6loc_id     integer ;
   t_lv5loc_id     integer ;
   t_lv4loc_id     integer ;
   t_lv3loc_id     integer ;
   t_lv2loc_id     integer ;
   t_lv1loc_id     integer ;
   t_lv7mast_id    integer ;

   tmp_lv10mast_id integer ;
   tmp_lv10merch_id integer ;
   tmp_ctr          integer ;
   t_ctr           integer ;

   t_pog_model_id  integer ;
   t_lv4loc_userid varchar(20) ;

   new_pos       	 numeric ;
   t_rows          numeric ;
   j               numeric ;
   ctr1            numeric ;
   unknown_ctr     numeric ;
   pog_id          integer ;
   ko_ctr          numeric ;
   t_group_id      integer ;

   t_min_date    	 date ;
   t_min_period  	 INTEGER ;
   t_min_cycle     INTEGER ;

begin
       open c_lastone ;
  loop
    fetch  c_lastone into t_store_loc_id, t_lv10mast_id, t_sum_pos ;
    exit when c_lastone%notfound;


    if t_sum_pos = 0 then
      insert into maxdata.temp_pos values (t_store_loc_id, t_lv10mast_id, 0) ;
      update maxdata.lv10mvmt
      set    positions = 1
      where  store_loc_id = t_store_loc_id and
             lv10mast_id = t_lv10mast_id ;
    end if ;

  end loop ;
  close c_lastone ;

     select min(period_start_date) into t_min_date from maxapp.period_lkup ;
     maxdata.f_get_period_cycle(t_min_date,t_min_period,t_min_cycle) ;

     unknown_ctr := 0 ;
     -- check for a DEFAULT record in the GROUPMASTER table
     -- if not found, create a new record

     select count(*) into unknown_ctr
     from   maxdata.groupmaster
     where  group_desc = 'DEFAULT' ;

  if unknown_ctr = 0 then
        maxapp.f_get_seq(91,0,t_group_id) ;
        insert into maxdata.groupmaster(group_master_id,group_desc)
        values(t_group_id, 'DEFAULT') ;
  elsif unknown_ctr = 1 then
        select group_master_id into t_group_id
        from   maxdata.groupmaster
        where  group_desc = 'DEFAULT' ;
  end if ;

  -- processing records from TEMP_POS

        unknown_ctr := 0 ;
        ko_ctr   := 0 ;

  for cur_rec in c_temp_pos
  loop
        t_lv10mast_id := cur_rec.lv10mast_id;
        t_lv4loc_id := cur_rec.lv4loc_id;
  begin
     begin
          select lv10mast_id
          into   tmp_lv10mast_id
          from   maxdata.lv10mast
          where  lv10mast_id = t_lv10mast_id;

     -- added for split master dev --rg 03/09
          select count(*)
          into   tmp_ctr
          from   maxdata.lv10merch
          where  lv10mast_id = tmp_lv10mast_id
          and    merch_lkup = 1;

          If tmp_ctr = 0 then
              maxapp.f_get_seq (10,13,tmp_lv10merch_id);
              insert into maxdata.lv10merch
               (lv10merch_id,
                lv10mast_id,
                merch_lkup)
              values
               (tmp_lv10mast_id,
                tmp_lv10merch_id,
                1);
          else
              select lv10merch_id
              into tmp_lv10merch_id
              from maxdata.lv10merch
              where  lv10mast_id = tmp_lv10mast_id
              and    merch_lkup = 1;

          end if;

       begin
        t_char_user9 := 'NDF' ;
             -- block for getting CHAR_USER9

        if  p_cmast_lvl = 1 then
          select lv1cmast.char_user9
          into   t_char_user9
          from   maxdata.lv1cmast, maxdata.lv10cat
          where  lv1cmast.lv1cmast_id = lv10cat.lv1cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
         end if;

        if p_cmast_lvl  = 2 then
          select lv2cmast.char_user9
          into   t_char_user9
          from   maxdata.lv2cmast, maxdata.lv10cat
          where  lv2cmast.lv2cmast_id = lv10cat.lv2cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
         end if;

        if p_cmast_lvl =3 then
          select lv3cmast.char_user9
          into   t_char_user9
          from   maxdata.lv3cmast, maxdata.lv10cat
          where  lv3cmast.lv3cmast_id = lv10cat.lv3cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
        end if;

        if p_cmast_lvl = 4 then
          select lv4cmast.char_user9
          into   t_char_user9
          from   maxdata.lv4cmast, maxdata.lv10cat
          where  lv4cmast.lv4cmast_id = lv10cat.lv4cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
        end if;

        if p_cmast_lvl =5 then
          select lv5cmast.char_user9
          into   t_char_user9
          from   maxdata.lv5cmast, maxdata.lv10cat
          where  lv5cmast.lv5cmast_id = lv10cat.lv5cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
        end if;

        if p_cmast_lvl = 6 then
          select lv6cmast.char_user9
          into   t_char_user9
          from   maxdata.lv6cmast, maxdata.lv10cat
          where  lv6cmast.lv6cmast_id = lv10cat.lv6cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
           end if;

	if p_cmast_lvl = 7 then
          select lv7cmast.char_user9
          into   t_char_user9
          from   maxdata.lv7cmast, maxdata.lv10cat
          where  lv7cmast.lv7cmast_id = lv10cat.lv7cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
           end if;

	if p_cmast_lvl = 8 then
          select lv8cmast.char_user9
          into   t_char_user9
          from   maxdata.lv8cmast, maxdata.lv10cat
          where  lv8cmast.lv8cmast_id = lv10cat.lv8cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
           end if;

	if p_cmast_lvl = 9 then
          select lv9cmast.char_user9
          into   t_char_user9
          from   maxdata.lv9cmast, maxdata.lv10cat
          where  lv9cmast.lv9cmast_id = lv10cat.lv9cmast_id and
                 lv10cat.lv10cat_id = (
                      select lv10cat_id
                      from   maxdata.lv10mast
                      where  lv10mast_id = t_lv10mast_id) ;
           end if;


-- this select should not fail

         exception
           when NO_DATA_FOUND then
              t_char_user9 := 'UNKNOWN' ;
              unknown_ctr  :=  unknown_ctr + 1 ;
           when OTHERS then
              maxdata.ins_import_log('P_UPD_POS','INFORMATION',
             'SELECT FROM LV10MAST failed lv4loc_id '||t_lv4loc_id||' lv10mast_id '||t_lv10mast_id,
              null,null,null) ;

       end;
-- end of block for getting CHAR_USER9

-- block for insert into LV10LOC

        begin
             select lv7loc_id
             into   t_lv7loc_id
             from   maxdata.lv7loc
             where  lv7loc_userid = t_char_user9 and
             lv4loc_id = t_lv4loc_id ;

          begin
             step := 1 ;
               select lv7mast_id
               into   t_lv7mast_id
               from   maxdata.lv7mast
               where  lv7mast_userid='DEFAULT' ;


             step := 2;
               select pog_model_id
               into   t_pog_model_id
               from   maxdata.pogmaster
               where  current_pog = 1 and
                      live_lv7loc_id = t_lv7loc_id ;

          exception
           when NO_DATA_FOUND then
             if step = 1 then
                maxdata.ins_import_log('P_UPD_POS','INFORMATION','DEFAULT row not found in
                                        LV7MAST',null,null,null) ;
             else
                   maxapp.f_get_seq(7,2,t_pog_model_id) ;
                   insert into maxdata.lv7loc
                          ( lv7loc_id,
                            last_update,
                            lv7loc_userid,
                            lv7mast_id,
                            lv6loc_id,
                            lv4loc_id,
                            record_type,
                            merch_arriv_period,
                            pog_status,
                            pog_default_date,
                        group_master_id )
                        values
                          ( t_pog_model_id,
                            sysdate,
                            substr(t_lv4loc_id||' '||t_char_user9,1,20),
                            t_lv7mast_id,
                            -1,
                            -1,
                            'M',
                            16,
                            'A',
                            t_min_date,
                        t_group_id ) ;

                   maxapp.f_get_seq(23,4096,pog_id) ;
                   insert into maxdata.pogmaster
                          ( pog_master_id,
                            pog_userid,
                            pog_lv4loc_id,
                            pog_model_id,
                            live_lv7loc_id,
                            pog_planned_start,
                            pog_actual_start,
                            pog_start_cycle,
                            pog_start_period,
                            lv7_version_id,
                            approval_status,
                        current_pog )
                        values
                          ( pog_id,
                            substr(t_lv4loc_id||' '||t_char_user9,1,20),
                            t_lv4loc_id,
                            t_pog_model_id,
                            t_lv7loc_id,
                            t_min_date,
                            t_min_date,
                            t_min_cycle,
                            t_min_period,
                            1,
                            1,
                        1 ) ;
             end if ;
          end ;

             maxapp.f_get_seq(10,2,j) ;
             insert into maxdata.lv10loc
                    ( lv10loc_id,
          		    last_update,
       		    lv10mast_id,
        		    lv9loc_id,
      		    lv8loc_id,
        		    lv7loc_id,
      		    lv6loc_id,
        		    lv5loc_id,
          		    lv4loc_id,
                      lv3loc_id,
                      lv2loc_id,
                      lv1loc_id,
         		    placed,
                      xcoord_facings,
                      zcoord_facings,
                   ypos_facings, lv10merch_id )
       		 values
                    ( j,
          		    sysdate,
                      t_lv10mast_id,
                      NULL,
                      NULL,
                      t_pog_model_id,
                      -1,
                      NULL,
                      -1,
                      NULL,
                      NULL,
                      NULL,
                      1,
          		    1,
          		    1,
          		 1 , tmp_lv10merch_id) ;

        exception
        when NO_DATA_FOUND then
                /* Select failed from LV7LOC */
           insert into maxdata.batmvmt_ko(lv10mast_id, store_loc_userid, ko_type, def_section, last_update)
           values(t_lv10mast_id, t_lv4loc_id, 'NO DEF SEC', t_char_user9, sysdate) ;
           ko_ctr := ko_ctr + 1 ;
         when OTHERS then
                ins_import_log('P_UPD_POS','INFORMATION',
                 'SELECT FROM LV7LOC failed lv4loc_id '||cur_rec.lv4loc_id||' order_code '||cur_rec.lv10mast_id||' char_user9 '||t_char_user9,
                 null,null,null);
        end;
      /* end of block for insert into LV10LOC */
  exception
        when NO_DATA_FOUND then
              /* Select failed from LV10MAST */
            insert into maxdata.batmvmt_ko(lv10mast_id, store_loc_userid, ko_type, last_update)
            values(t_lv10mast_id, t_lv4loc_id, 'NO MAST', sysdate) ;
            ko_ctr := ko_ctr + 1 ;
         when OTHERS then
              ins_import_log('P_UPD_POS','INFORMATION',
                'SELECT FROM LV10MAST failed lv4loc_id '||cur_rec.lv4loc_id||' order_code '||cur_rec.lv10mast_id,
                 null,null,null);
  end;
  exception
        when OTHERS then
          ins_import_log('P_UPD_POS','INFORMATION',
            'lv4loc_id '||cur_rec.lv4loc_id||' order_code '||cur_rec.lv10mast_id||' char_user9 '||t_char_user9,
            null,null,null) ;
  end;

        delete from maxdata.temp_pos
        where  lv4loc_id = t_lv4loc_id and
               lv10mast_id = t_lv10mast_id ;

  end loop ;

  insert into maxdata.import_log(log_id, log_level, log_text)
    select distinct 'P_UPD_POS','INFORMATION',
           'Create Store '||lv4loc.lv4loc_userid||' Section '||batmvmt_ko.def_section
    from   maxdata.lv4loc, maxdata.batmvmt_ko
    where  batmvmt_ko.store_loc_userid = lv4loc.lv4loc_id ;

END ;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_POS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_UPD_POS" TO "MAXUSER";
