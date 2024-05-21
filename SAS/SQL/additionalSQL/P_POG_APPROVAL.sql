--------------------------------------------------------
--  DDL for Procedure P_POG_APPROVAL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_POG_APPROVAL" as

/*--------------------------------------------------
-- Change History

$Log: 4184_p_pog_approval.sql,v $
Revision 1.1.8.1.6.1  2008/10/30 19:21:56  vejang
checked in from 612 CD location on D12176

Revision 1.2  2006/01/18 15:26:04  raabuh
Apply 5.6.1 and 5.6.2 space scripts


-- V5.6.1
-- 5.6.1-047 02/17/05   helmi		  Added delete from pogmaster for closed stores
-- 5.6.1-047 02/08/05   Rashmi		#17008  Added commit at the end of the procedure to make
--						behaviour consistent with sql and UDB.
-- 5.6.1-047 01/27/05	Diwakar		#17008. Added calling procedure p_update_lcosed_store.
-- V5.4
-- 5.4.0-031 12/10/02    Sachin          Ported from 5.3.4 - 143_04. No changes made.
-- 5.3.4
-- 11/25/02 DR removed unapproved pog cursor as it updates the column with null
--		Changed period_lkup to lv5time table
--------------------------------------------------*/

  cursor c_live_pog is
    select distinct live_lv7loc_id
    from   pogmaster
    where  approval_status= 1 ;

  cursor c_pog (p_live_lv7loc_id pogmaster.live_lv7loc_id%TYPE) is
    select pog_master_id, pog_actual_start, pog_start_period,
           pog_start_cycle, pog_end_date, pog_end_period, pog_end_cycle
    from   pogmaster
    where  live_lv7loc_id = p_live_lv7loc_id and approval_status = 1
    order  by pog_actual_start desc ;

  t_start_date 	date ;
  t_end_date  	date ;
  t_min_end_date date;
  t_start_period  number ;
  t_start_cycle	number ;
  t_end_period	number ;
  t_end_cycle	number ;

begin

  maxdata.p_update_closed_store();

  for cur_rec1 in c_live_pog loop
    t_start_period  := 0 ;
    t_start_cycle	:= 0 ;
    t_end_date 	:= null ;
    t_end_period	:= 0 ;
    t_end_cycle	:= 0 ;

    for cur_rec2 in c_pog (cur_rec1.live_lv7loc_id) loop
      -- set start date to actual start
      t_start_date := cur_rec2.pog_actual_start ;

      -- get start period and cycle
      f_get_period_cycle(t_start_date, t_start_period, t_start_cycle) ;

      Select min(lv5time_start_date) into t_min_end_date From maxapp.lv5time;

      -- get end period and cycle moved from after update by Dan

      if t_end_date = t_min_end_date then
         Select lv5time_id, cycle_id
 	  into t_end_period, t_end_cycle
	  From maxapp.lv5time
	  where lv5time_start_date = t_min_end_date;
      else
        f_get_period_cycle(t_end_date, t_end_period, t_end_cycle) ;
      end if ;

      -- set values
      update pogmaster
      set    pog_start_period = t_start_period,
             pog_start_cycle = t_start_cycle,
             pog_end_date = t_end_date,
             pog_end_period = t_end_period,
             pog_end_cycle = t_end_cycle
      where  pog_master_id = cur_rec2.pog_master_id ;

      t_end_date := t_start_date ;

    end loop ;
  end loop ;


delete from maxdata.pogmaster
where pog_lv4loc_id in (
	select lv4loc_id from maxdata.lv4loc where close_date <= SYSDATE)
and pog_model_id in (
	select lv7loc_id from maxdata.lv7loc where lv7loc_userid = 'CLOSED_STORE_POG')
and  approval_status = 1
and pog_end_date IS  NULL;

commit;

end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_POG_APPROVAL" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_POG_APPROVAL" TO "MAXUSER";
