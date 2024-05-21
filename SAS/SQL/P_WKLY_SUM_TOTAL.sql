--------------------------------------------------------
--  DDL for Procedure P_WKLY_SUM_TOTAL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_SUM_TOTAL" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin


Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log 
(Process_Id,Process_Step,Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,200,'Step 20:Process Cartesian table for Retail/Quantity:p_wkly_sum_total',Sysdate,'wkly_sum_total','I');
Commit;



--Execute Immediate 'Drop Index Wst_Pk';
--Execute Immediate 'drop index wst_fk';
  execute immediate 'truncate table Wkly_sum_Totals';

 Dbms_Stats.Gather_Table_Stats(Ownname => 'ERIC',tabname => 'wkly_sum_totals');



 Update Wkly_Sum_Sites 

  Set Processed_Ind = Null, Processed_Date = Null, Start_Date = Null; 
 Commit; 







  For Asitegroup In (Select Distinct Site_Group From Wkly_Sum_Sites 
                     Where Processed_Ind Is Null Order By Site_Group) 
  Loop 

    Update Wkly_Sum_Sites 

       Set Start_Date = Sysdate 
    WHERE site_group = asitegroup.site_group;
    commit;



    Insert /*+ APPEND */ Into Wkly_Sum_Totals 
    (Site_Id,Sku_Key,Merchandising_Year,Merchandising_Week,Mark_Up_Perm,
     Mark_Down_Perm, Mark_Down_Pos, Sales_Shrink,Sales_Sold_Price,Purchases,
     Transfers_Retail, Inven_qty,adjustments_retail) 
    SELECT 

      Site_Id, 

      Sku_Key, 

      Merchandising_Year, 

      Merchandising_Week, 

      Mark_Up_Perm, 

      Mark_Down_Perm, 

      Mark_Down_Pos, 

      Sales_Shrink,

      Sales_Sold_Price,

      Purchases,

      Transfers_Retail,

      Inven_Qty,

      Adjustments_retail

    From ( 

     Select 

        C.Site_Id, 

        c.Sku_Key, 

        Merchandising_Year, 
        Merchandising_Week, 
        Sum(Mark_Up_Perm) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Mark_Up_Perm, 
        Sum(Mark_Down_Perm) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Mark_Down_Perm, 
        Sum(Mark_Down_Pos) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Mark_Down_Pos, 
        Sum( Sales_Shrink  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Sales_Shrink, 
        Sum( Sales_Sold_Price  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Sales_Sold_Price, 
        Sum( Purchases  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Purchases, 
        Sum( Transfers_Retail  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Transfers_Retail, 
        Sum( Inven_Qty  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Inven_Qty, 
        Sum( Adjustments_Retail  ) Over ( 
          Partition By C.Site_Id, C.Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Adjustments_Retail 
    From Wkly_Tmp_Sum_Calc C 
    Where 

      Site_Id In (Select Site_Id From Wkly_Sum_Sites 
                   Where Site_Group = Asitegroup.Site_Group) 
    ) Base 

  Where 

   (Base.Mark_Up_Perm <> 0 Or 
    Base.Mark_Down_Perm <> 0 Or 
    Base.Mark_Down_Pos <> 0 Or 
    Base.Sales_Shrink <> 0 Or
    Base.Sales_Sold_Price <> 0 Or
    Base.Purchases <> 0 Or

    Base.Transfers_Retail <> 0 Or
    Base.Inven_Qty <> 0 or 
    base.adjustments_retail <> 0) and
    Exists (Select 1 From Wkly_Sas_Prod_Time_Pd 
            Where Merch_Year = Base.Merchandising_Year 
              AND merch_week = base.merchandising_week);


  Update Wkly_Sum_Sites 

    Set Processed_Ind = '1', 
    Processed_Date = Sysdate 
  Where Site_Group = Asitegroup.Site_Group;   
  Commit; 

End Loop; 


/*

  Execute Immediate 'Create Unique Index Wst_Pk On 
    Wkly_Sum_Totals(Site_Id, Sku_Key, Merchandising_Year, Merchandising_Week)';
  execute Immediate 'Create Bitmap Index Wst_Fk On 
    Wkly_Sum_Totals(Merchandising_Year,Merchandising_Week) Local Nologging
    Tablespace Ericdata Parallel (Degree 12 Instances Default)';
*/


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 200 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_sas_bop_eop();', Sysdate, Null);
--Dbms_Job.Submit(Jobno, 'P_Wkly_sas_bop_eop_sp();', Sysdate, Null);
Commit;


END P_WKLY_SUM_TOTAL;       

/
