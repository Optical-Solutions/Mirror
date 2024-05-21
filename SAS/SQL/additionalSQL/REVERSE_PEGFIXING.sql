--------------------------------------------------------
--  DDL for Procedure REVERSE_PEGFIXING
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."REVERSE_PEGFIXING" (l_phys_type NUMBER)
as

	l_lv9loc_id INTEGER;
 	l_peg_column NUMBER(10,0);
 	l_peg_row NUMBER(10,0);

	cursor cur_join is
	select dev.lv9loc_id, tmp.peg_column, tmp.peg_row
		from maxdata.lv9loc dev , maxdata.TEMP_lv9LOC_dev TMP
	where dev.lv9loc_id = tmp.lv9loc_id
		and tmp.phys_type = l_phys_type;

BEGIN

	Open cur_join;
	loop
		Fetch cur_join into l_lv9loc_id,  l_peg_column, l_peg_row;
		exit when cur_join%notfound;

		update maxdata.lv9loc
		set peg_column 	= l_peg_column,
		peg_row 	= l_peg_row
		where lv9loc_id = l_lv9loc_id;

	end loop;
	close cur_join;

END;

/

  GRANT EXECUTE ON "MAXDATA"."REVERSE_PEGFIXING" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."REVERSE_PEGFIXING" TO "MAXUSER";
