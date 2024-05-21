--------------------------------------------------------
--  DDL for Procedure P_GET_PLANOGRAMS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_PLANOGRAMS" (
		lIdGen number,
		lLocId number,
		level int,
		fromdatetoChar varchar2,
		todatetoChar varchar2,
		lDateMethod int,
  	        pogtype char,
  	        proc_Status out int ,
  	        ErrMsg out varchar2
)as

-- V5.6
-- 05/06/04	Diwakar	#16705	Added alias names for in line queries for datementhod = 9
--			#16720	Added conditions to datemethods 8,9 and 10
-- 04/05/04	Diwakar	Added New column pog_actual_start to insert statement of pog_item_pog.
--			Added pog_actual_start column to all cursor statements.
-- 03/26/04	Diwakar	Added curosor statements for web next only plans and web add next plans (if lDateMethod 9 and 10)
-- V5.3.4
-- 7/30/03  Rashmi  bug fix for 15060 from Apar.

-- General Declarations.
            l_lIdGen number(10,0);
            l_lLocId number(10,0);
            l_pogtype          char(1);
            l_live_lv7loc_id number(10,0);
            l_pog_lv4loc_id number(10,0);
            l_pog_master_id number(10,0);
            Error integer;
            fromdate date;
            todate date;
            prevLive7LocId number(10,0);
	    t_web_distrib_prep_time number(4);
	    t_pog_actual_start date;

-- Declare a user Defined Type Record as return type

   TYPE pog_rec IS RECORD (
            lIdGen number(10,0),
            pog_model_id number(10,0),
            pogtype char(1),
            live_lv7loc_id number(10,0),
            pog_lv4loc_id number(10,0),
            pog_master_id  number(10,0),
	    pog_actual_start date );

-- Declare a UD Data Type of a cursor variable so that its definition varies with Date Method Input.

            TYPE pog_cursor IS REF CURSOR RETURN pog_rec;

-- Declare Cursor variable

            pog_cur pog_cursor;
Begin


          if (lDateMethod < 8) then

		-- Convert Date in char to Date type
            	fromdate := to_date(fromdatetoChar, 'MM-DD-YYYY HH:MI:SS');

		-- Convert Date in char to Date type
	        todate := to_date(todatetoChar, 'MM-DD-YYYY HH:MI:SS');

	   End if;

	--get the WEB_DISTRIB_PREP_TIME from userpref
	BEGIN
	SELECT  TO_NUMBER(value_1) INTO t_web_distrib_prep_time
	FROM maxapp.userpref WHERE key_1 = 'WEB_DISTRIB_PREP_TIME';
	END;


-- Find out the right set to be inserted basing on date method.

-- Open cursor variable with appropriate Select

-- Then run a in iterations with the result set fetch and insert into pog_item_pog

            if (lDateMethod = 0) then
                        OPEN pog_cur  FOR
                        Select  lIdGen, pog_model_id, pogtype, live_lv7loc_id, pog_lv4loc_id, pog_master_id, pog_actual_start
                    From maxdata.pogmaster
                        Where live_lv7loc_id in
                                (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                      where a.lv7loc_id = lLocId
                                                and a.record_type = 'L'
                                                and b.lv7mast_userid <> 'DEFAULT'
                                                and b.lv7mast_id = a.lv7mast_id)
                        order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 1) then

                       OPEN pog_cur  FOR
                        Select lIdGen, pog_model_id,  pogtype, live_lv7loc_id, pog_lv4loc_id, pog_master_id, pog_actual_start
                        From maxdata.pogmaster
                        Where live_lv7loc_id in
                                     (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                                where a.lv7loc_id =  lLocId
                                                            and a.record_type = 'L'
                                                            and b.lv7mast_userid <> 'DEFAULT'
                                                            and b.lv7mast_id = a.lv7mast_id)
                                    and ((pog_actual_start is not NULL and pog_actual_start between fromdate and todate)
                                                or (pog_actual_start is NULL and pog_planned_start between fromdate  and todate))
                        order by live_lv7loc_id, pog_actual_start desc;


            elsif (lDateMethod = 2) then

                        OPEN pog_cur  FOR
                        Select  lIdGen, pog_model_id, pogtype, live_lv7loc_id, pog_lv4loc_id, pog_master_id, pog_actual_start
                        From maxdata.pogmaster
                        Where live_lv7loc_id in
                                     (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                                where a.lv7loc_id = lLocId
                                                            and a.record_type = 'L'
                                                            and b.lv7mast_userid <> 'DEFAULT'
                                                            and b.lv7mast_id = a.lv7mast_id)
                                    and ((pog_actual_start is not NULL and pog_actual_start between fromdate  and todate )
                                                or (pog_actual_start is NULL and pog_planned_start between fromdate and todate))
                                    and approval_status = 1
                        order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 3) then

                        OPEN pog_cur  FOR
                        Select  lIdGen, pog_model_id, pogtype, live_lv7loc_id, pog_lv4loc_id, pog_master_id, pog_actual_start
                        From maxdata.pogmaster
                        Where live_lv7loc_id in
                                    (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                      where a.lv7loc_id =  lLocId
                                                and a.record_type = 'L'
                                                and b.lv7mast_userid <> 'DEFAULT'
                                                and b.lv7mast_id = a.lv7mast_id)
                                                and ((pog_actual_start is not NULL and pog_actual_start between fromdate  and todate)
                                                  or (pog_actual_start is NULL and pog_planned_start between fromdate  and todate))
                                    and approval_status = 0
                        order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 4) then

                        OPEN pog_cur  FOR
                        Select lIdGen, pog_model_id, pogtype, live_lv7loc_id, pog_lv4loc_id,pog_master_id, pog_actual_start
                From maxdata.pogmaster
                        Where live_lv7loc_id in  (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                                                        where a.lv7loc_id = lLocId
                                                                                    and a.record_type = 'L'
                                                                                    and b.lv7mast_userid <> 'DEFAULT'
                                                                                    and b.lv7mast_id = a.lv7mast_id)
                                    and ((pog_actual_start is not NULL and pog_actual_start <= fromdate )
                                                or (pog_actual_start is NULL and pog_planned_start <= fromdate ))
                                    and ( (pog_end_date >= todate)  or (pog_end_date is null) )
                                    and approval_status = 1
                       order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 5) then

                        OPEN pog_cur  FOR
                        Select lIdGen, pog_model_id, pogtype, live_lv7loc_id, pog_lv4loc_id, pog_master_id, pog_actual_start
                    From maxdata.pogmaster
                        Where live_lv7loc_id in
                                    (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                   where a.lv7loc_id = lLocId
                                                and a.record_type = 'L'
                                                and b.lv7mast_userid <> 'DEFAULT'
                                                and b.lv7mast_id = a.lv7mast_id)
                                                and ((pog_actual_start is not NULL and pog_actual_start <= fromdate  )
                                                          or (pog_actual_start is NULL and pog_planned_start <= fromdate ))
                                    and ( (pog_end_date >= todate )  or (pog_end_date is null) )
                                    and approval_status = 0
                        order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 6) then
                        OPEN pog_cur  FOR
                        Select  lIdGen, pogm.pog_model_id, pogtype,pogm.live_lv7loc_id, pogm.pog_lv4loc_id,pogm.pog_master_id, pogm.pog_actual_start
                        From maxdata.pogmaster pogm, maxdata.lv7loc lv7loc
                        Where pogm.pog_model_id = lv7loc.lv7loc_id
--                                  ( lv7loc.user_modify_flag is NOT NULL and lv7loc.user_modify_flag = 'Y' ) -- redundant check
                                    and lv7loc.user_modify_flag = 'Y'
                                    and pogm.live_lv7loc_id in
                                            (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                                  where a.lv7loc_id =  lLocId
                                                                         and a.record_type = 'L'
                                                            and b.lv7mast_userid <> 'DEFAULT'
                                                            and b.lv7mast_id = a.lv7mast_id)
                                    and ((pogm.pog_actual_start is not NULL and pogm.pog_actual_start between fromdate  and todate)
                                                  or (pogm.pog_actual_start is NULL and pogm.pog_planned_start between fromdate  and todate))
                                    and pogm.approval_status = 1
                        order by live_lv7loc_id, pog_actual_start desc;

            elsif (lDateMethod = 7) then

                        OPEN pog_cur  FOR
                        Select  lIdGen, pogm.pog_model_id, pogtype,pogm.live_lv7loc_id, pogm.pog_lv4loc_id, pogm.pog_master_id, pogm.pog_actual_start
                        From maxdata.pogmaster pogm, maxdata.lv7loc lv7loc
                        Where pogm.pog_model_id = lv7loc.lv7loc_id
                                    and (lv7loc.user_modify_flag is NULL or lv7loc.user_modify_flag <> 'Y' )
                                    and pogm.live_lv7loc_id in
                                     (select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
                                                where a.lv7loc_id =  lLocId
                                                            and a.record_type = 'L'
                                                            and b.lv7mast_userid <> 'DEFAULT'
                                                            and b.lv7mast_id = a.lv7mast_id)
                                    and ((pogm.pog_actual_start is not NULL and pogm.pog_actual_start between fromdate and todate)
                                         or (pogm.pog_actual_start is NULL and pogm.pog_planned_start between fromdate and todate))
                                    and pogm.approval_status = 1
                        order by live_lv7loc_id, pog_actual_start desc;

              elsif (lDateMethod = 8) then  -- Web Current and Next Plans
	    	--Add for Web distribution
	    	BEGIN

			OPEN pog_cur  FOR
			Select lIdGen, A.pog_model_id, pogtype, A.live_lv7loc_id, A.pog_lv4loc_id, A.pog_master_id, A.pog_actual_start
			From maxdata.pogmaster A
			Where A.live_lv7loc_id in
				(select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
				where a.lv7loc_id = lLocId
				and a.record_type = 'L'
				and b.lv7mast_userid <> 'DEFAULT'
				and b.lv7mast_id = a.lv7mast_id )
				and A.pog_actual_start =
					(select max(B.pog_actual_start)
					from maxdata.pogmaster B
					where ((sysdate - B.pog_actual_start) >=0
					and ((B.pog_end_date - sysdate) >=0 or B.pog_end_date is null )) --Current planogram -- System Date > Actual Start Date < End Date
				and B.live_lv7loc_id = A.live_lv7loc_id
				and B.approval_status = 1)
			AND  A.approval_status = 1
			Union
			Select  lIdGen, A.pog_model_id, pogtype, A.live_lv7loc_id, A.pog_lv4loc_id, A.pog_master_id, A.pog_actual_start
			From maxdata.pogmaster A
			Where A.live_lv7loc_id in
				(select lv7loc_id from maxdata.lv7loc a, maxdata.lv7mast b
				where 	a.lv7loc_id = lLocId
				and a.record_type = 'L'
				and b.lv7mast_userid <> 'DEFAULT'
				and b.lv7mast_id = a.lv7mast_id )
				and A.pog_actual_start =
					(select min(B.pog_actual_start)
					from maxdata.pogmaster B
					where (((sysdate + (NVL(B.pog_set_days,0) + t_web_distrib_prep_time))
						- B.pog_actual_start) >=0
					and (B.pog_actual_start - sysdate) > 0) --Current Date < Actual Start Date and Distribution Date + Current Date >= Actual Start Date
					and B.live_lv7loc_id = A.live_lv7loc_id
					and B.approval_status = 1)
			and A.approval_status = 1
			order by pog_actual_start desc ;

		  END;

		elsif (lDateMethod = 9) then	-- Web Add Next Plans

		Begin
			-- This distribution mode picks both current and next pogs in the section that have changed only


			OPEN pog_cur  FOR
			SELECT lIdGen, POG.pog_model_id, pogtype, POG.live_lv7loc_id, POG.pog_lv4loc_id, POG.pog_master_id, POG.pog_actual_start
			FROM
				(SELECT  lIdGen lIdGen, P1.pog_model_id, pogtype pogtype, P1.live_lv7loc_id, P1.pog_lv4loc_id, P1.pog_master_id,P1.pog_actual_start
				FROM maxdata.pogmaster P1
				WHERE P1.live_lv7loc_id in (
						SELECT lv7loc_id
						FROM maxdata.lv7loc LIVE, maxdata.lv7mast MAST
						WHERE LIVE.lv7loc_id = lLocId
						AND LIVE.record_type = 'L'
						AND MAST.lv7mast_userid <> 'DEFAULT'
						AND MAST.lv7mast_id = LIVE.lv7mast_id)
				AND P1.pog_actual_start = (
						SELECT max(P2.pog_actual_start)
						FROM maxdata.pogmaster P2
						WHERE P2.pog_actual_start <= SYSDATE
						AND (P2.pog_end_date >= SYSDATE OR P2.pog_end_date is NULL)
						AND P2.live_lv7loc_id = P1.live_lv7loc_id
						AND P2.approval_status = 1)
				AND  P1.approval_status = 1
				AND EXISTS (SELECT 1
						FROM  maxdata.pogmaster P3
						WHERE P3.live_lv7loc_id = lLocId
						AND P3.pog_actual_start > SYSDATE
						AND (P3.pog_actual_start <= (SYSDATE + NVL(P3.pog_set_days,0) + t_web_distrib_prep_time))
						AND P3.approval_status = 1
						)
			UNION
				SELECT  lIdGen lIdGen, P4.pog_model_id,  pogtype pogtype, P4.live_lv7loc_id, P4.pog_lv4loc_id,P4.pog_master_id,P4.pog_actual_start
				FROM maxdata.pogmaster P4
				WHERE P4.live_lv7loc_id in (
					SELECT lv7loc_id
					FROM maxdata.lv7loc LOC, maxdata.lv7mast MAST
					WHERE LOC.lv7loc_id = lLocId
					AND LOC.record_type = 'L'
					AND MAST.lv7mast_userid <> 'DEFAULT'
					AND MAST.lv7mast_id = LOC.lv7mast_id)
				AND P4.pog_actual_start = (
					SELECT min(P5.pog_actual_start)
					FROM maxdata.pogmaster P5
					WHERE P5.pog_actual_start > SYSDATE
					AND P5.pog_actual_start <= (SYSDATE + NVL(P5.pog_set_days,0) + t_web_distrib_prep_time)
					AND P5.live_lv7loc_id = P4.live_lv7loc_id
					AND P5.approval_status = 1)
				AND P4.approval_status = 1
			) POG
			order by pog.pog_actual_start ;


		End; -- if (@lDateMethod = 9)

		elsif (lDateMethod = 10) then	-- Web Next only plans
		Begin
			-- Retrieve pogs from Highlighted sections only.

			OPEN pog_cur  FOR
			Select lIdGen, POG.pog_model_id, pogtype, POG.live_lv7loc_id, POG.pog_lv4loc_id, POG.pog_master_id, POG.pog_actual_start
			From maxdata.pogmaster POG
			Where POG.live_lv7loc_id in (
				Select lv7loc_id From maxdata.lv7loc LIVE, maxdata.lv7mast MAST
				Where LIVE.lv7loc_id = lLocId
				and LIVE.record_type = 'L'
				and MAST.lv7mast_userid <> 'DEFAULT'
				and MAST.lv7mast_id = LIVE.lv7mast_id
			)
			AND POG.pog_actual_start = (
				Select min(PG.pog_actual_start)
				From maxdata.pogmaster PG
				Where PG.pog_actual_start <= (sysdate + NVL(PG.pog_set_days,0) + t_web_distrib_prep_time)
				and  PG.pog_actual_start > SYSDATE
				and PG.live_lv7loc_id = POG.live_lv7loc_id
				and PG.approval_status = 1
			)
			AND  POG.approval_status = 1 ;


		End ; -- if (@lDateMethod = 10)


		end if;

-- Intialization

            prevLive7LocId := -9999;

            loop
                        -- Check if Cursor is open

                        if pog_cur%ISOPEN then

	                        -- Fetch

                                    Fetch pog_cur into l_lIdGen,l_lLocId,l_pogtype,l_live_lv7loc_id,l_pog_lv4loc_id,l_pog_master_id, t_pog_actual_start;

	                        -- Exit if cursor reached end

                                    exit when pog_cur%notfound;

                        else

                                -- Exit if Cursor is not open, or Cursor has no definition because of DateMethod being passed

                                    proc_Status := 0;
                                    ErrMsg := 'Invalid Date Method';
                                    goto EndLoop;


                        end if;

                        -- If current and previous live_lv7loc_id are not the same(filtering for duplicates) then insert

                        if (prevLive7LocId != l_live_lv7loc_id)   then

                                    insert into maxdata.pogitem_pog
                                    (request_id,
                                     lv7loc_id,
                                     pog_type,
                                     live_lv7loc_id,
                                     pog_lv4loc_id,
                                     pog_master_id,
                                     pog_actual_start)
                                    values
                                    (l_lIdGen,
                                     l_lLocId,
                                     l_pogtype,
                                     l_live_lv7loc_id,
                                     l_pog_lv4loc_id,
                                     l_pog_master_id,
                                     t_pog_actual_start);

				if (lDateMethod > -1 and lDateMethod <8) THEN
                                    prevLive7LocId := l_live_lv7loc_id;
                          	end if;

                        end if;

            end loop;

            if pog_cur%ISOPEN then
                        close pog_cur;
            end if;

            proc_Status :=1 ;

<<EndLoop>>
null;

exception
            when others then

                        Error := SQLCODE;
                        ErrMsg := SUBSTR(SQLERRM, 1, 255);
                        proc_Status := 0;


End;

/

  GRANT EXECUTE ON "MAXDATA"."P_GET_PLANOGRAMS" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_GET_PLANOGRAMS" TO "MAXUSER";
