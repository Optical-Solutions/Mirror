--------------------------------------------------------
--  DDL for Procedure P_GET_DEF_SECTIONS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_GET_DEF_SECTIONS" 
AS


   p_cmast_lvl integer;
   unknown_ctr     numeric ;
   t_group_id      integer ;


begin

     unknown_ctr := 0 ;

     -- check for a DEFAULT record in the GROUPMASTER table
     -- if not found, create a new record
	-- Truncate Table maxdata.xref_cat_cmast;
delete from maxdata.xref_cat_cmast;
commit;
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


	select cmast_lvl into p_cmast_lvl from maxapp.mmax_config;


	if  p_cmast_lvl = 1 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv1cmast.lv1cmast_id, 		lv1cmast.char_user9, 1
             	from   maxdata.lv1cmast, maxdata.lv10cat
		where  lv10cat.lv1cmast_id = lv1cmast.lv1cmast_id;

      elsif p_cmast_lvl  = 2 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv2cmast.lv2cmast_id, 		lv2cmast.char_user9, 2
             	from   maxdata.lv2cmast, maxdata.lv10cat
		where  lv10cat.lv2cmast_id = lv2cmast.lv2cmast_id  ;
	elsif p_cmast_lvl =3 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv3cmast.lv3cmast_id, 		lv3cmast.char_user9, 3
             	from   maxdata.lv3cmast, maxdata.lv10cat
		where  lv10cat.lv3cmast_id = lv3cmast.lv3cmast_id  ;

	elsif p_cmast_lvl = 4 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv4cmast.lv4cmast_id, 		lv4cmast.char_user9, 4
             	from   maxdata.lv4cmast, maxdata.lv10cat
		where  lv10cat.lv4cmast_id = lv4cmast.lv4cmast_id  ;

	elsif p_cmast_lvl =5 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv5cmast.lv5cmast_id, 		lv5cmast.char_user9, 5
             	from   maxdata.lv5cmast, maxdata.lv10cat
		where  lv10cat.lv5cmast_id = lv5cmast.lv5cmast_id  ;
	elsif p_cmast_lvl = 6 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv6cmast.lv6cmast_id, 		lv6cmast.char_user9, 6
             	from   maxdata.lv6cmast, maxdata.lv10cat
		where  lv10cat.lv6cmast_id = lv6cmast.lv6cmast_id;
	elsif p_cmast_lvl = 7 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv7cmast.lv7cmast_id, 		lv7cmast.char_user9, 7
             	from   maxdata.lv7cmast, maxdata.lv10cat
		where  lv10cat.lv7cmast_id = lv7cmast.lv7cmast_id  ;

	elsif p_cmast_lvl = 8 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv8cmast.lv8cmast_id, 		lv8cmast.char_user9, 8
             	from   maxdata.lv8cmast, maxdata.lv10cat
		where  lv10cat.lv8cmast_id = lv8cmast.lv8cmast_id  ;
      elsif p_cmast_lvl = 9 then

		insert into maxdata.xref_cat_cmast( lv10cat_id, lvcmast_id, 		char_user9, lvcmast_lvl)
	      select lv10cat.lv10cat_id, lv9cmast.lv9cmast_id, 		lv9cmast.char_user9, 9
             	from   maxdata.lv9cmast, maxdata.lv10cat
		where  lv10cat.lv9cmast_id = lv9cmast.lv9cmast_id  ;

	end if;

	update xref_cat_cmast
	set 	group_id = t_group_id;

	commit;

END ;

/
