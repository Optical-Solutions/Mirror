--------------------------------------------------------
--  DDL for Procedure P_WKLY_SAS_INV_SP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_SAS_INV_SP" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

Parm_Start_Week Number;

Parm_Start_Year Number;


Begin

/*

Process is part of the original way we use to calculate BOP/EOP.

Final Step in calculating EOP Cost

Since it has been learned that to get the correct EOP retail you need to 
follow a special RIM formula.

*/

Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Select Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) 
Where Rownum = 1;


Insert Into Sas_Process_Log 
(Process_Id,Process_Step,Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,180,'Step 18:Aggregate Cost Totals by weeks:p_wkly_sas_inv',Sysdate,'wkly_sas_inv','I');
commit;


  Execute Immediate 'truncate table wkly_sas_inv'; 
  Update Wkly_Sas_Sites Set Processed_Ind = Null, Processed_Date = Null, Start_Date = Null; 
  Commit; 


  For Asitegroup In (Select Distinct Site_Group From Wkly_Sas_Sites 
                     Where Processed_Ind Is Null Order By Site_Group) 
  Loop 

    Update Wkly_Sas_Sites Set Start_Date = Sysdate 
    WHERE site_group = asitegroup.site_group;
    commit;


    Insert /*+ APPEND */ Into Wkly_Sas_Inv 
     (Site_Id,Sku_Key,Merchandising_Year,Merchandising_Week,
      Inventory_On_Week,Retail_On_Week,Cost_On_Week) 
    SELECT 

      Site_Id, 

      Sku_Key, 

      Merchandising_Year, 

      Merchandising_Week, 

      Inventory_On_Week, 

      Retail_On_Week, 

      Cost_On_Week 

    From ( 

      SELECT 

        Site_Id, 

        Sku_Key, 

        Merchandising_Year, 
        Merchandising_Week, 
        SUM(inven_move_qty) OVER ( 
          Partition By Site_Id, Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Inventory_On_Week, 
        Sum(Retail_Price) Over ( 
          Partition By Site_Id, Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Retail_On_Week, 
        Sum(Cost) Over ( Partition By Site_Id, Sku_Key 
        Order By Merchandising_Year , Merchandising_Week ) Cost_On_Week 
      From Wkly_Tmp_Sas_Inventory_Calc C 
      Where 

        Site_Id In (Select Site_Id From Wkly_Sas_Sites 
                    Where Site_Group = Asitegroup.Site_Group) 
        ) Base 

    Where Inventory_On_Week <> 0 And 
          Exists (Select 1 From Wkly_Sas_Prod_Time_Pd 
                  Where Merch_Year = Base.Merchandising_Year And 
                        merch_week = base.merchandising_week);

    Update Wkly_Sas_Sites Set Processed_Ind = '1', Processed_Date = Sysdate 
    Where Site_Group = Asitegroup.Site_Group;   
    Commit; 

  End Loop; 



 /* getting week from previous year */

Insert Into Wkly_Sas_Inv (Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, Merchandising_Year, Merchandising_Week)
Select Site_Id, Sku_Key, 0 Inventory_On_Week, 
      0 Retail_On_Week , nvl(qty_cost,0) Cost_On_Week, 2016 merchandising_year, 52 merchandising_week
    From

      LDL_EOP_RP_COST  Spc      
      Join (Select Style_Id, Min(Sku_Key) Sku_Key 
            From Sas_Product_Master
            Group By Style_Id) Spg1 On (Spc.Style_Id = Spg1.Style_Id);
Commit;




Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 180 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_Sum_Cart();', Sysdate, Null);
Commit;



END P_WKLY_SAS_INV_SP;

/
