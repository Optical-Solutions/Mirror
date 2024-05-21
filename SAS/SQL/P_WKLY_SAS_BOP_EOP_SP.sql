--------------------------------------------------------
--  DDL for Procedure P_WKLY_SAS_BOP_EOP_SP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_SAS_BOP_EOP_SP" 
Is 
                        
                           
P_Process_Id Number;

Parm_Start_Week Number;

Parm_Start_year Number;

Parm_End_Week Number;

Parm_Period Number;

Parm_year Number;

Jobno Binary_Integer;


BEGIN

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 
Select Merch_Week Into Parm_End_Week From (Select * From Wkly_Sas_Prod_Time_Pd Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;
Select Merch_Week, merch_year Into Parm_Start_Week, Parm_Start_year From (Select * From Wkly_Sas_Prod_Time_Pd Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,210,'Step 21:Process EOP using RIM formula:p_wkly_sas_bop_eop',Sysdate,'wkly_sas_bop_eop','I');
commit;


Execute Immediate 'truncate table New_Wkly_Eop'; 

/* SQL manipulation to get the min period for the past 5 weeks even during 
   cross merch year eg 2015 thru 2016 */
select merchandising_period, merchandising_year into parm_period, parm_year from
 (select * from

  (select * from 

   (select merchandising_year, merchandising_week, 
            merchandising_period 
     from  merchandising_calendars
     where week_ending_date <= sysdate
     order by 1 desc, 2 desc)
   where rownum < 6) 

  order by 1, 2)

where rownum = 1;







For rec In (Select * From Wkly_Sas_Prod_Time_Pd)
Loop

Insert Into New_Wkly_Eop

(Site_Id, Sku_Key,  Merchandising_Year,  Merchandising_Week, Retail_On_Week, inventory_on_week)
Select 

      Site_Id, Sku_Key, 

      Merchandising_Year, 

      Merchandising_Week, 

      Sum(

       (nvl(retail_on_week,0)  
      + Nvl(Purchases,0) 

      + Nvl(Transfers_Retail,0) 
      + Nvl(Mark_Up_Perm,0)
      - Nvl(Sales_Sold_Price,0)
      - Nvl(Mark_Down_Perm,0) 
      - Nvl(Mark_Down_Pos,0)       
      - Nvl(Sales_Shrink,0)
      - nvl(Adjustments_retail,0))) Retail_On_Week,
      sum(inventory_on_week)
    From

    ((Select 

      spc.Site_Id, Spg1.Sku_Key, Spc.Style_Id,
      Rec.Merch_Year Merchandising_Year, 
      rec.merch_week Merchandising_Week,  
      qty inventory_on_week, 
      nvl(eop_retail,0) retail_on_week , 0 Cost_On_Week, 0 Mark_Down_Perm, 0 Mark_Up_Perm, 0 Mark_Down_Pos,
     0 Sales_Shrink, 0 Sales_Sold_Price, 0 Purchases,  0 Transfers_Retail, 0 Adjustments_retail
    From

      LDL_EOP_RP_COST Spc      
      Join (Select Style_Id, Min(Sku_Key) Sku_Key 
            From Sas_Product_Master
            Group By Style_Id) Spg1 On (Spc.Style_Id = Spg1.Style_Id))

    Union all

    (Select Site_Id, spg2.Sku_Key, spm2.style_id, Merchandising_Year, Merchandising_Week, inven_qty Inventory_On_Week, 
      0 Retail_On_Week, 0 Cost_On_Week, Mark_Down_Perm, Mark_Up_Perm, Mark_Down_Pos,
      Case When 12 = (Select Distinct M.Merchandising_Period From Merchandising_Calendars M
                      Where M.Merchandising_Week = Rec.Merch_Week And
                            M.Merchandising_Year = Rec.Merch_Year)
      Then 0

      Else Sales_Shrink

      End Sales_Shrink , Sales_Sold_Price, Purchases,  Transfers_Retail, Adjustments_Retail 
     From

      Wkly_Sum_Totals Wst

      Join Sas_Product_Master Spm2 On (Spm2.Sku_Key = Wst.Sku_Key)
      Join (Select Style_Id, Min(Sku_Key) Sku_Key From Sas_Product_Master
            group by style_id) spg2 on (spm2.style_id = spg2.style_id)
      Where

          Merchandising_Year = Rec.Merch_Year And
          Merchandising_Week = rec.merch_week)) Base

   Group By        

   Site_Id, Sku_Key,  Merchandising_Year, Merchandising_Week having (sum(inventory_on_week) <> 0 or Sum(
       (nvl(retail_on_week,0)  
      + Nvl(Purchases,0) 

      + Nvl(Transfers_Retail,0) 
      + Nvl(Mark_Up_Perm,0)
      - Nvl(Sales_Sold_Price,0)
      - Nvl(Mark_Down_Perm,0) 
      - Nvl(Mark_Down_Pos,0)       
      - Nvl(Sales_Shrink,0)
      - nvl(Adjustments_retail,0))) <> 0);
Commit;

End Loop;



Insert Into New_Wkly_Eop

 (Site_Id, Sku_Key,  Merchandising_Year,  Merchandising_Week, Retail_On_Week, Inventory_On_Week)
Select Site_Id, Sku_Key, 2016 Merchandising_Year,  52 Merchandising_Week, nvl(eop_retail,0) Retail_On_Week, qty Inventory_On_Week
From 

 LDL_EOP_RP_COST Spc      

      Join (Select Style_Id, Min(Sku_Key) Sku_Key 
            From Sas_Product_Master
            Group By Style_Id) Spg1 On (Spc.Style_Id = Spg1.Style_Id);

      Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 210 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_merge_eop_cost();', Sysdate, Null);
commit;


END P_WKLY_SAS_BOP_EOP_SP;

/
