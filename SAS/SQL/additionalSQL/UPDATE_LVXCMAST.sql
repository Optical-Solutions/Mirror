--------------------------------------------------------
--  DDL for Procedure UPDATE_LVXCMAST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."UPDATE_LVXCMAST" (in_level  IN integer, in_entity IN Integer)
AS
Begin

if in_level = 1 and in_entity = 9 then

Update maxdata.lv1cmast
Set
(
name,
role_lkup,
loyalty_index,
mrkt_share,
mrkt_growth,
total_inv_cost,
total_inv_units,
total_sales,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
role_lkup,
loyalty_index,
mrkt_share,
mrkt_growth,
total_inv_cost,
total_inv_units,
total_sales,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv1cmast.lv1cmast_id
)

where lv1cmast.lv1cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv1cmast.lv1cmast_id);

end if;

if in_level = 2 and in_entity = 9 then

Update maxdata.lv2cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv2cmast.lv2cmast_id
)

where lv2cmast.lv2cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv2cmast.lv2cmast_id);

end if;

if in_level = 3 and in_entity = 9 then

Update maxdata.lv3cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv3cmast.lv3cmast_id
)

where lv3cmast.lv3cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv3cmast.lv3cmast_id);

end if;

if in_level = 4 and in_entity = 9 then

Update maxdata.lv4cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv4cmast.lv4cmast_id
)

where lv4cmast.lv4cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv4cmast.lv4cmast_id);

end if;

if in_level = 5 and in_entity = 9 then

Update maxdata.lv5cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv5cmast.lv5cmast_id
)

where lv5cmast.lv5cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv5cmast.lv5cmast_id);



end if;

if in_level = 6 and in_entity = 9 then

Update maxdata.lv6cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,

char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv6cmast.lv6cmast_id
)

where lv6cmast.lv6cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv6cmast.lv6cmast_id);

end if;

if in_level = 7 and in_entity = 9 then

Update maxdata.lv7cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv7cmast.lv7cmast_id
)

where lv7cmast.lv7cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv7cmast.lv7cmast_id);

end if;

if in_level = 8 and in_entity = 9 then

Update maxdata.lv8cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv8cmast.lv8cmast_id
)

where lv8cmast.lv8cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv8cmast.lv8cmast_id);

end if;

if in_level = 9 and in_entity = 9 then

Update maxdata.lv9cmast
Set
(
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
)=
(Select
name,
num_user1,
num_user3,
num_user2,
num_user4,
num_user5,
num_user6,
date_user7,
date_user8,
char_user9,
char_user10,
char_user11,
char_user12,
char_user13,
bigchar_user14,
bigchar_user15
From maxdata.dm_lvxcmast where
dm_lvxcmast.lvxcmast_id
= lv9cmast.lv9cmast_id
)

where lv9cmast.lv9cmast_id =
( select lvxcmast_id  from maxdata.dm_lvxcmast where dm_lvxcmast.lvxcmast_id = lv9cmast.lv9cmast_id);

end if;

delete from maxdata.dm_lvxcmast;
End;

/

  GRANT EXECUTE ON "MAXDATA"."UPDATE_LVXCMAST" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."UPDATE_LVXCMAST" TO "MAXUSER";
