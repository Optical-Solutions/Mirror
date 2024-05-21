--------------------------------------------------------
--  DDL for Procedure P_UPD_POG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_POG" 
as

/* This proc is made by combining p_upd_pog_status,  p_*/

    cursor c_pog is
    select live_lv7loc_id, pog_model_id, pog_master_id, pog_actual_start, pog_start_period,
           pog_start_cycle, pog_end_date, pog_end_period, pog_end_cycle
	    from   pogmaster
    where  approval_status = 1
 	   order  by live_lv7loc_id, pog_actual_start desc;    --, pog_model_id, pog_end_date ;


  t_prev_live_lv7loc_id number := 0;
  t_pog_status char(1) ;
  t_start_date 	date ;
  t_end_date  	date := null;
  t_start_period  number ;
  t_start_cycle	number ;
  t_end_period	number := 0 ;
  t_end_cycle	number := 0;
  prv_id                    integer ;
  t_pog_model_id            integer ;



begin

  prv_id := 0 ;
  t_pog_status := NULL ;

      for cur_rec2 in c_pog loop

	    t_start_period  := 0 ;
	    t_start_cycle   := 0 ;

      -- set start date to actual start

      t_start_date := cur_rec2.pog_actual_start ;
      t_pog_model_id := cur_rec2.pog_model_id;

      -- get start period and cycle

      maxdata.f_get_period_cycle(t_start_date, t_start_period, t_start_cycle) ;

      -- get end period and cycle moved from after update by Dan

      if t_end_date = to_date('01-JAN-1980','DD-MON-YYYY') then

        t_end_period := 1 ;
        t_end_cycle  := 1980 ;

      elsif (t_prev_live_lv7loc_id != cur_rec2.live_lv7loc_id ) then

      	t_end_date := null;
        t_end_period := null ;
        t_end_cycle  := null ;

--        f_get_period_cycle(t_end_date, t_end_period, t_end_cycle) ;

      end if ;

            -- set values

      update pogmaster
      set    pog_start_period = t_start_period,
             pog_start_cycle = t_start_cycle,
             pog_end_date = t_end_date,
             pog_end_period = t_end_period,
             pog_end_cycle = t_end_cycle
      where  pog_master_id = cur_rec2.pog_master_id
      		and live_lv7loc_id = cur_rec2.live_lv7loc_id ;



	if (t_start_date is not null) then

	    if prv_id <> t_pog_model_id then

	       t_pog_status := NULL ;
	       prv_id := t_pog_model_id ;

	    end if ;

	    if ((t_start_date <= sysdate) and (( t_end_date > sysdate) or (t_end_date is NULL) )) then

	       t_pog_status := 'A' ;

	       prv_id := 0 ;

	    elsif t_end_date < sysdate then

	       t_pog_status := 'I' ;

	    end if ;


	    if ((prv_id <> t_pog_model_id) and (t_pog_status is not NULL)) then

	       update lv7loc

	              set   pog_status = t_pog_status

	       where  lv7loc_id = t_pog_model_id ;

	    end if ;

 	end if;

 	t_end_date := t_start_date ;
 	t_end_period := t_start_period;
 	t_end_cycle := t_start_cycle;

 	t_prev_live_lv7loc_id := cur_rec2.live_lv7loc_id;

 end loop ;

end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_POG" TO "MAXAPP";
