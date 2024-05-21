--------------------------------------------------------
--  DDL for Procedure MRI_PREP_FOR_PURGE10
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."MRI_PREP_FOR_PURGE10" is
obj_count number;

begin

execute immediate 'drop table sas_barcodes_sku';

execute immediate 'create table sas_barcodes_sku as select * from rdiusr.v_sas_barcodes_sku@mc2r';

execute immediate 'create unique index idx_sas_barcodes_sku on sas_barcodes_sku(sku_key)';

execute immediate 'drop table lv10mast_bk';

execute immediate 'create table lv10mast_bk as select * from lv10mast';

update lv10mast lv10
set lv10.active_lkup = 1
where exists (select 1 from sas_barcodes_sku rms where rms.sku_key = lv10.order_code);

update lv10mast lv10
set lv10.active_lkup = -1
where not exists (select 1 from sas_barcodes_sku rms where rms.sku_key = lv10.order_code);

execute immediate 'truncate table monorder';

select count(*) into obj_count from user_tables where lower(table_name) = 'mdpu_purge_product';

if obj_count > 0 then
execute immediate 'truncate table mdpu_purge_product';
end if;

commit;

end;

/
