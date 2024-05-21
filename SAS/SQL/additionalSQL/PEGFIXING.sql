--------------------------------------------------------
--  DDL for Procedure PEGFIXING
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."PEGFIXING" 
(l_phys_type NUMBER)
as
	cursor Cur_join  is
	select loc.lv9loc_id,
		loc.lv9mast_id ,loc.x_coord ,
		mast.width, mast.height, loc.y_coord ,
			mast.phys_type, peg_column, peg_row
		from maxdata.lv9loc loc , maxdata.lv9mast mast
	where 	loc.lv9mast_id =mast.lv9mast_id
		and mast.phys_type = l_phys_type;

	l_lv9loc_id INTEGER;
	l_lv9mast_id  INTEGER;
	l_lv9loc_x_co NUMBER(16,9);
	l_lv9mast_width NUMBER(16,9);
	l_lv9mast_height NUMBER(16,9);
	l_lv9loc_y_co NUMBER(16,9);
	l_lv9mast_phys_type NUMBER(2,0);
 	l_peg_column NUMBER(10,0);
 	l_peg_row NUMBER(10,0);
 	l_Mtr_to_inch NUMBER(20,10);

BEGIN

	l_Mtr_to_inch := 39.370079;

/* Get the columns for matching mastids in the loc table at level 9 */

/* Run through the cursor */

	Open cur_join;
	LOOP
		Fetch cur_join into l_lv9loc_id, l_lv9mast_id ,l_lv9loc_x_co ,
		l_lv9mast_width, l_lv9mast_height, l_lv9loc_y_co ,
		l_lv9mast_phys_type, l_peg_column, l_peg_row;

		Exit when cur_join%notfound;
	/* Created a temp table to store the modifications */

		insert into maxdata.temp_lv9loc_dev
		values(l_lv9loc_id, l_peg_row, l_peg_column, l_lv9mast_phys_type);

/* Addtional Phys_types can be added in if else ladder to accomodate varying calculations*/

		if (l_phys_type = 4) then

	/* Update the columns to fix the problem */


			update maxdata.lv9loc
			set 	peg_column 	= ( FLOOR(ROUND(( l_Mtr_to_inch * (l_lv9loc_x_co + (0.5 * l_lv9mast_width) )) , 0 )) - 1),
				peg_row 	= (FLOOR(ROUND(( l_Mtr_to_inch * (l_lv9loc_y_co + (0.5 * l_lv9mast_height))),0 ) ) -1)

			where 	lv9mast_id =l_lv9mast_id
				and lv9loc_id = l_lv9loc_id;

		end if;

            if (l_phys_type = 6) then

			update maxdata.lv9loc
			set 	peg_column = FLOOR(ROUND((l_Mtr_to_inch * (x_coord + ((0.5 * l_lv9mast_width) - 0.0127))),0)) - 1,
				peg_row = FLOOR( ROUND((l_Mtr_to_inch * (y_coord)),0))
			where 	lv9mast_id =l_lv9mast_id
				and lv9loc_id = l_lv9loc_id;
		end if;

	END LOOP;
	close cur_join;
end;

/

  GRANT EXECUTE ON "MAXDATA"."PEGFIXING" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."PEGFIXING" TO "MAXUSER";
