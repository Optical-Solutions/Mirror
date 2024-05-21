--------------------------------------------------------
--  DDL for Procedure P_UPD_POG_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_UPD_POG_STATUS" 
AS

/* This procedure sets the live planograms to active basing on the effective Dates
Author:
Review History:
	Modified to populate alloc_xxx in pogmaster  06/29/1999 - suresh
	Modified to use live alloc_sq_meters inplace of the same from model
									07/28/1999 - Suresh
	Modified the cusrsor logic and rewritten the procedure to fix the bug
			reported at SMC		- Santosh 04/27/2000
	Modified the procedure to fix the bug reported at PAMIDA for model Pog status - Santosh 08/27/2000
*/



  t_pog_status char(1) ;
  prv_id                    integer ;
  t_pog_model_id            integer ;
  t_pog_actual_start_date   date ;
  t_pog_end_date            date ;
  t_live_lv7loc_id 	    integer;
  t_active_pog_ctr	    integer;
  t_inactive_pog_ctr	    integer;

    cursor   c_pogmodel is
    select distinct (pog_model_id)
    from   pogmaster;

 /* Cursor to select all model planograms which are assigned */

    cursor   c_pogmodel1 is
    select pog_actual_start, live_lv7loc_id, pog_model_id, pog_end_date, pog_master_id
    from   pogmaster
    where  pog_model_id = t_pog_model_id
	and pog_actual_start is not NULL
	order by live_lv7loc_id desc;


  /* variables for populating alloc_xxx columns newly added 06/29/1999 */

  t_on_dsp_front maxdata.lv7loc.on_dsp_front%type :=0;
  t_on_dsp_back maxdata.lv7loc.on_dsp_back%type :=0;
  t_on_dsp_left maxdata.lv7loc.on_dsp_left%type :=0;
  t_on_dsp_right maxdata.lv7loc.on_dsp_right%type :=0;
  t_height maxdata.lv7loc.height%type :=0;
  t_width maxdata.lv7loc.width%type :=0;
  t_depth maxdata.lv7loc.depth%type :=0;
  t_alloc_sq_mtrs maxdata.lv7loc.alloc_sq_meters%type :=0;
  t_alloc_cubic_meters maxdata.pogmaster.alloc_cubic_meters%type :=0;
  t_alloc_dsp_sqmeters maxdata.pogmaster.alloc_dsp_sqmeters%type :=0;
  t_alloc_flr_sqmeters maxdata.pogmaster.alloc_flr_sqmeters%type :=0;
  t_alloc_linear_meters maxdata.pogmaster.alloc_linear_meter%type :=0;
  t_pog_master_id maxdata.pogmaster.pog_master_id%type;

begin
  prv_id := 0 ;
  t_pog_status := NULL ;
  t_active_pog_ctr := 0;
  t_inactive_pog_ctr := 0;

/*  Fetch one after the other  */

  open c_pogmodel;
  	loop
		fetch  c_pogmodel into
	       		t_pog_model_id;
      	exit when c_pogmodel%notfound;

  t_pog_status := 'P' ;
  t_active_pog_ctr := 0;
  t_inactive_pog_ctr := 0;

  open c_pogmodel1;
  	loop
		fetch  c_pogmodel1 into
	      t_pog_actual_start_date, t_live_lv7loc_id, t_pog_model_id,  t_pog_end_date, t_pog_master_id ;
      	exit when c_pogmodel1%notfound;

	 /* If the start date < today and end > today or end is null then it is active */
      	if t_pog_actual_start_date <= sysdate and
			( t_pog_end_date > sysdate or t_pog_end_date is NULL ) then
		t_pog_status := 'A' ;
		t_active_pog_ctr := t_active_pog_ctr + 1;

	 /* If end date < today then inactive */
        elsif t_pog_end_date < sysdate then
      		t_pog_status := 'I' ;
		t_inactive_pog_ctr := t_inactive_pog_ctr + 1;
       	end if ;



    /* This section is added to populate alloc_xxxx columns added for R3 06/29/1999 Suresh */
    /* As the values are for lv10finc only Active Planograms are considered */

	if t_pog_status = 'A' then

    /* Select all the space attributes fromm lv7loc for the model */

		select on_dsp_front, on_dsp_back, on_dsp_left, on_dsp_right, height, width, depth
		  into
	  		t_on_dsp_front, t_on_dsp_back, t_on_dsp_left, t_on_dsp_right, t_height, t_width, t_depth
		from maxdata.lv7loc
      			where  lv7loc_id = t_live_lv7loc_id;

  -- Added to aply alloc_sq_meters from live than from model 07/28/1999 - Suresh

		select alloc_sq_meters into t_alloc_sq_mtrs from maxdata.lv7loc
			where lv7loc_id = t_live_lv7loc_id;

  /* Calculate alloc_xxx attribs */

     		t_alloc_cubic_meters := t_alloc_sq_mtrs * t_height;
      		t_alloc_dsp_sqmeters := ((t_on_dsp_front + t_on_dsp_back) *
					(t_height * t_width)) + ((t_on_dsp_left +
					t_on_dsp_right ) * ( t_height * t_depth));
                t_alloc_flr_sqmeters := t_alloc_sq_mtrs;
       		t_alloc_linear_meters := ( (t_on_dsp_front + t_on_dsp_back) * t_width ) +
       					( (  t_on_dsp_left + t_on_dsp_right ) * t_depth);

	 /* update the current active planogram of that model with space attributes */

		update pogmaster
      		set 	alloc_cubic_meters = t_alloc_cubic_meters,
			alloc_dsp_sqmeters = t_alloc_dsp_sqmeters,
			alloc_flr_sqmeters = t_alloc_flr_sqmeters,
			alloc_linear_meter = t_alloc_linear_meters
		where live_lv7loc_id  = t_live_lv7loc_id;
	end if ;

	end loop ;
  close c_pogmodel1;

	/* update lv7loc with status */

	/* Check no. of active live planograms and inactive planograms
	   If atleast one active planograms, then set the model pog status as Active */

	if t_active_pog_ctr = 0 and t_inactive_pog_ctr = 0 then
		  t_pog_status := 'P' ;
	end if;

	if t_active_pog_ctr > 0 then
		  t_pog_status := 'A' ;
	end if;

	if t_active_pog_ctr = 0 and t_inactive_pog_ctr > 0 then
		  t_pog_status := 'I' ;
	end if;



	update lv7loc
  		set   pog_status = t_pog_status
		where  lv7loc_id = t_pog_model_id ;

	commit;

	end loop ;
  close c_pogmodel;
  commit;
end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_UPD_POG_STATUS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_UPD_POG_STATUS" TO "MAXUSER";
