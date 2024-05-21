--------------------------------------------------------
--  DDL for Procedure P_WKLY_INS_CALC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_INS_CALC" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

Parm_Start_Week Number;

Parm_Start_Year Number;

Begin


/*

Process is part of the original way we use to calculate BOP/EOP.

This way will only work for COST and is still used just for cost.

Since it has been learned that to get the correct EOP retail you need to 
follow a special RIM formula.

*/


Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id 

where Process_Type = 'WEEKLY'; 

Select Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) 
Where Rownum = 1;



Insert Into Sas_Process_Log 
   (Process_Id,Process_Step,Process_Name,
    Process_Start_Time,Process_Table,Process_Ind)
Values 

   (P_Process_Id,60,'Step 6:Prepare Cartesian table for Cost:p_wkly_ins_calc',
    Sysdate,'Review Procedure - Multiple Tables','I');
Commit;



Execute Immediate 'drop table wkly_tmp_sas_inventory_calc';
Execute Immediate 'CREATE TABLE wkly_tmp_sas_inventory_calc
PARALLEL (degree 12)

TABLESPACE ERICDATA

PARTITION BY RANGE (merchandising_week)
   INTERVAL ( 1 )

   (PARTITION p1 VALUES LESS THAN (2))
   NOLOGGING

AS

   SELECT site_id,

          sku_key,

          inven_move_qty,

          retail_price,

          cost,

          merchandising_year,
          merchandising_week
     FROM wkly_im_sas_grp

';


Execute Immediate 'create index wtsic_idx0 on wkly_tmp_sas_inventory_calc(site_id, sku_key, merchandising_year, merchandising_week)';

/*

Get one week prior so that aggregated total are added correctly
*/



If Parm_Start_Week = 1

Then

 /* getting week from previous year */
 Insert Into Wkly_Tmp_Sas_Inventory_Calc 
 Select Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
 From

   Sas_Prod_Complete_2012 

 Where Merchandising_Week = (Select Max(Merchandising_Week) From Merchandising_Calendars 
                             Where Merchandising_Year = (Parm_Start_Year - 1) )And
       Merchandising_Year = (Parm_Start_Year - 1);                
else

 Insert Into Wkly_Tmp_Sas_Inventory_Calc 
 Select Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
 From

   Sas_Prod_Complete_2012 

 Where Merchandising_Week = Parm_Start_Week -1 And
       Merchandising_Year = Parm_Start_Year;                
end if;

commit;


execute Immediate 'Truncate Table Wkly_Sas_Sites';
insert into wkly_sas_sites

    SELECT site_id, mod(rownum,30) site_group , 
           '0' processed_ind, 
           to_date('19000101','YYYYMMDD') processed_date, 
           to_date('19000101','YYYYMMDD') start_date
    FROM (

            Select Distinct Site_Id 
            FROM wkly_tmp_sas_inventory_calc
            ORDER BY 1

         );

commit;


update wkly_sas_sites 

   set processed_ind = null, 
       processed_date = null, 
       Start_Date = Null;

commit;



Execute Immediate 'Truncate Table Wkly_Sas_Styles';

Insert Into Wkly_Sas_Styles
Select Distinct Style_Id 

From Wkly_Tmp_Sas_Inventory_Calc I 
join sas_product_master s on (s.sku_key = I.Sku_Key);
commit;




Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 60 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_cart_calc();', Sysdate, Null);
Commit;


END P_WKLY_INS_CALC;

/
