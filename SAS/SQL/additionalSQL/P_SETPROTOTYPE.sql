--------------------------------------------------------
--  DDL for Procedure P_SETPROTOTYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_SETPROTOTYPE" 
as

-- Change History:
--
-- V5.3.3
-- 06/19/02	Rashmi	Ver_loc_link_id shd be same as lvxloc_id.

	t_lv4loc_userid varchar2(25);
	t_prototype_userid varchar2(25);
        bk_lv6loc_userid varchar2(25);
        bk_lv7loc_userid varchar2(25);
	t_lv4loc_id varchar2(25);
	pt_lv4loc_id integer;
	pt_lv5loc_id integer;
	t_lv5loc_id integer;
	pt_lv6loc_id  integer;
	t_lv6loc_id integer;
	pt_lv7loc_id integer;
	t_lv7loc_id integer;
	t_lv3loc_id decimal(10,0);
	t_lv2loc_id decimal(10,0);
	t_lv1loc_id decimal(10,0)  ;
	t_lv4mast_id decimal(10,0);
        bk_lv5loc_id integer;
        bk_lv6loc_id integer;
	pt_height decimal(16,9);
	pt_width decimal(16,9);
	pt_depth decimal(16,9);

	ctr  integer;

begin

	delete from lv4loc_tmp;
	commit;

	FOR c1 in ( SELECT   lv4loc_id, Prototype_Userid, lv3loc_id, lv2loc_id, lv1loc_id, lv4mast_id
		    FROM 	maxdata.Prototype_Stores) loop

		SELECT  lv4loc_id, height, width, depth
		INTO pt_lv4loc_id, pt_height, pt_width, pt_depth
		FROM maxdata.lv4loc
		WHERE lv4loc_userid =  c1.prototype_userid
                AND lv1loc_id in ( select lv1loc_id from lv1loc where num_user1  = 3) ;

		delete from lv5loc_tmp;
		commit;

		UPDATE maxdata.lv4loc
		SET height = pt_height,
		    width = pt_width,
		    depth = pt_depth,
		    changed_by_batch = 0
		where changed_by_batch = 70
		and lv4loc_id = c1.lv4loc_id;

		Insert INTO maxdata.lv5loc_tmp
		SELECT * FROM maxdata.lv5loc
		WHERE lv4loc_id = pt_lv4loc_id;

		FOR c2 in (SELECT lv5loc_id
			FROM maxdata.lv5loc_tmp) loop

			maxapp.f_get_seq(5, 2, t_lv5loc_id);

			UPDATE maxdata.lv5loc_tmp
			SET lv5loc_id = t_lv5loc_id,
		            lv4loc_id = c1.lv4loc_id,
				lv3loc_id = c1.lv3loc_id,
				lv2loc_id = c1.lv2loc_id,
				lv1loc_id = c1.lv1loc_id,
                                ver_loc_link_id = t_lv5loc_id
			where lv5loc_id = c2.lv5loc_id;

                end loop;


			delete from lv6loc_tmp;
			commit;

			Insert into maxdata.lv6loc_tmp
			SELECT * FROM maxdata.lv6loc
			WHERE lv4loc_id = pt_lv4loc_id ;

			FOR c3 in (SELECT lv6loc_id, lv5loc_id
				FROM maxdata.lv6loc_tmp) loop

				maxapp.f_get_seq(6, 2, t_lv6loc_id);

                                Select lv5loc_id into t_lv5loc_id
				FROM lv5loc_tmp
                                WHERE lv5loc_userid
				        = ( select lv5loc_userid from lv5loc
					where lv5loc_id = c3.lv5loc_id)
				AND lv4loc_id = c1.lv4loc_id;

				UPDATE maxdata.lv6loc_tmp
				SET lv6loc_id = t_lv6loc_id,
					lv5loc_id = t_lv5loc_id,
					lv4loc_id = c1.lv4loc_id ,
				lv3loc_id = c1.lv3loc_id,
				lv2loc_id = c1.lv2loc_id,
				lv1loc_id = c1.lv1loc_id,
                                ver_loc_link_id = t_lv6loc_id
					WHERE lv6loc_id = c3.lv6loc_id;
                        end loop;


				delete from lv7loc_tmp;
				commit;

				insert INTO maxdata.lv7loc_tmp
				SELECT * FROM maxdata.lv7loc
				WHERE lv4loc_id = pt_lv4loc_id ;

			FOR c4 in (SELECT lv7loc_id , lv6loc_id, lv5loc_id, lv4loc_id
                       			   FROM maxdata.lv7loc_tmp) loop

					maxapp.f_get_seq(7, 2, t_lv7loc_id);

				IF c4.lv5loc_id is not null or c4.lv5loc_id <> -1 THEN
                                Select lv5loc_id into t_lv5loc_id
				FROM lv5loc_tmp
				WHERE lv5loc_userid
				= ( select lv5loc_userid from lv5loc
					where lv5loc_id = c4.lv5loc_id)
				        AND lv4loc_id = c1.lv4loc_id;
				ELSE
					t_lv5loc_id := NULL;
				END IF;
				IF c4.lv6loc_id is not null or c4.lv6loc_id <> -1 THEN
			        Select lv6loc_id into t_lv6loc_id
				FROM lv6loc_tmp
				WHERE lv6loc_userid
				= ( select lv6loc_userid from lv6loc
					where lv6loc_id = c4.lv6loc_id)
				        AND lv4loc_id = c1.lv4loc_id;

				ELSE
					t_lv6loc_id := NULL;
				END IF;
					UPDATE maxdata.lv7loc_tmp
					SET lv7loc_id = t_lv7loc_id,
						lv6loc_id = t_lv6loc_id,
						lv5loc_id = t_lv5loc_id,
						lv4loc_id = c1.lv4loc_id ,
						lv3loc_id = c1.lv3loc_id,
						lv2loc_id = c1.lv2loc_id,
						lv1loc_id = c1.lv1loc_id,
                                                ver_loc_link_id = t_lv7loc_id
					WHERE lv7loc_id = c4.lv7loc_id;

			end loop;

                                        INSERT INTO  maxdata.lv5loc
			                       SELECT * FROM lv5loc_tmp;

                                        INSERT INTO maxdata.lv6loc
				                SELECT * FROM lv6loc_tmp;

					INSERT INTO maxdata.lv7loc
						SELECT * FROM maxdata.lv7loc_tmp;



	END loop;
END;

/

  GRANT EXECUTE ON "MAXDATA"."P_SETPROTOTYPE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_SETPROTOTYPE" TO "DATAMGR";
  GRANT EXECUTE ON "MAXDATA"."P_SETPROTOTYPE" TO "MAXUSER";
