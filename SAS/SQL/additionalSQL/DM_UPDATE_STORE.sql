--------------------------------------------------------
--  DDL for Procedure DM_UPDATE_STORE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."DM_UPDATE_STORE" (in_level IN integer, in_entity IN Integer)
AS
t_lv3loc_id number(10, 0);
t_lv2loc_id number(10, 0);
t_lv1loc_id number(10, 0);
t_lv4mast_id number(10, 0);
 t_lv4loc_id number(10, 0);
ctr integer;

CURSOR C1 IS SELECT lv4mast_id,
		lv4loc_id,
		lv3loc_id,
		lv2loc_id,
		lv1loc_id
	FROM maxdata.dm_lv4loc;
Begin

if in_level = 4 and in_entity = 1 then

Update maxdata.lv4mast
Set
(
name,
long_name,
prim_type,
prim_filename,
height,
width,
depth,
weight_capacity,
stock_method,
facet1_attach,
phys_type,
image_back,
image_bottom,
image_front,
image_left,
image_right,
image_top,
line_back,
line_bottom,
line_top,
line_front,
line_left,
line_right,
num_user1,
num_user2,
num_user3,
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
bigchar_user15)

=
(Select
name,
long_name,
prim_type,
prim_filename,
height,
width,
depth,
weight_capacity,
stock_method,
facet1_attach,
phys_type,
image_back,
image_bottom,
image_front,
image_left,
image_right,
image_top,
line_back,
line_bottom,
line_top,
line_front,
line_left,
line_right,
num_user1,
num_user2,
num_user3,
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
From maxdata.dm_lv4mast where
dm_lv4mast.lv4mast_id
= lv4mast.lv4mast_id
)

where lv4mast.lv4mast_id =
( select lv4mast_id  from maxdata.dm_lv4mast where dm_lv4mast.lv4mast_id = lv4mast.lv4mast_id);

Delete from maxdata.dm_lv4mast;
commit;

end if;


if in_level = 4 and in_entity = 2 then
open c1;

	loop
		fetch c1 into t_lv4mast_id,
		 t_lv4loc_id,
		 t_lv3loc_id,
		 t_lv2loc_id,
		 t_lv1loc_id;

		exit when c1%NOTFOUND;

	 SELECT count(*) into ctr  FROM maxdata.lv7loc WHERE lv4loc_id =t_lv4loc_id;

	IF ctr > 0   THEN
		UPDATE lv7loc
		SET  lv3loc_id = t_lv3loc_id,
		lv2loc_id = t_lv2loc_id,
		lv1loc_id = t_lv1loc_id
		WHERE lv4loc_id = t_lv4loc_id;
	END IF;

	 SELECT count(*) into ctr  FROM maxdata.lv6loc WHERE lv4loc_id =t_lv4loc_id;

	IF ctr > 0   THEN
		UPDATE lv6loc
		SET  lv3loc_id = t_lv3loc_id,
		lv2loc_id = t_lv2loc_id,
		lv1loc_id = t_lv1loc_id
		WHERE lv4loc_id = t_lv4loc_id;
	END IF;

	 SELECT count(*) into ctr  FROM maxdata.lv5loc WHERE lv4loc_id =t_lv4loc_id;

	IF ctr > 0   THEN
		UPDATE lv5loc
		SET  lv3loc_id = t_lv3loc_id,
		lv2loc_id = t_lv2loc_id,
		lv1loc_id = t_lv1loc_id
		WHERE lv4loc_id = t_lv4loc_id;
	END IF;

Update maxdata.lv4loc
Set
(
changed_by_batch,
project_type,
archive_flag,
lv4mast_id,
lv3loc_id,
lv2loc_id,
lv1loc_id,
model_id,
facet1_attach,
start_date,
end_date,
x_coord,
y_coord,
z_coord,
placed,
total_items,
total_units,
total_caps,
overlvls_id,
xcoord_just_lkup,
ycoord_just_lkup,
zcoord_just_lkup,
top_over,
left_over,
right_over,
front_over,
loc_color,
orient,
rotate,
slope,
xcoord_gap,
ycoord_gap,
zcoord_gap,
min_xcoord_gap,
min_ycoord_gap,
min_zcoord_gap,
max_xcoord_gap,
max_ycoord_gap,
max_zcoord_gap,
anchor_lkup,
used_cubic_meters,
used_dsp_sqmeters,
used_linear_meters,
target_days_supply,
address_1,
address_2,
city,
state,
zip,
longitude,
latitude,
num_user1,
num_user2,
num_user3,
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
bigchar_user15,
height,
width,
depth,
shape_lkup_id,
aso_filename,
aso_root_filename,
aso_area,
aso_area_id,
tot_lv7dsp,
tot_lv7lin,
close_date,
lv3loc_userid,
lv2loc_userid,
lv1loc_userid,
ATTRIB1,
ATTRIB2,
ATTRIB3,
ATTRIB4,
ATTRIB5,
ATTRIB6,
LV13LOC_ID,
LV23LOC_ID)

=
(Select
changed_by_batch,
project_type,
archive_flag,
lv4mast_id,
lv3loc_id,
lv2loc_id,
lv1loc_id,
model_id,
facet1_attach,
start_date,
end_date,
x_coord,
y_coord,
z_coord,
placed,
total_items,
total_units,
total_caps,
overlvls_id,
xcoord_just_lkup,
ycoord_just_lkup,
zcoord_just_lkup,
top_over,
left_over,
right_over,
front_over,
loc_color,
orient,
rotate,
slope,
xcoord_gap,
ycoord_gap,
zcoord_gap,
min_xcoord_gap,
min_ycoord_gap,
min_zcoord_gap,
max_xcoord_gap,
max_ycoord_gap,
max_zcoord_gap,
anchor_lkup,
used_cubic_meters,
used_dsp_sqmeters,
used_linear_meters,
target_days_supply,
address_1,
address_2,
city,
state,
zip,
longitude,
latitude,
num_user1,
num_user2,
num_user3,
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
bigchar_user15,
height,
width,
depth,
shape_lkup_id,
aso_filename,
aso_root_filename,
aso_area,
aso_area_id,
tot_lv7dsp,
tot_lv7lin,
close_date,
lv3loc_userid,
lv2loc_userid,
lv1loc_userid,
ATTRIB1,
ATTRIB2,
ATTRIB3,
ATTRIB4,
ATTRIB5,
ATTRIB6,
LV13LOC_ID,
LV23LOC_ID
From maxdata.dm_lv4loc where
dm_lv4loc.lv4loc_id
= t_lv4loc_id
)

WHERE  lv4loc.lv4loc_id = t_lv4loc_id;
END loop;
CLOSE C1;
Delete from maxdata.dm_lv4loc;
commit;
end if;
End;

/
