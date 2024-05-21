--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_EOP_RETAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_EOP_RETAIL" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 240,'Step 24:Merge/Insert Process EOP Retail/Quantity:p_wkly_merge_eop_retail',Sysdate,'sas_prod_complete','I');
Commit;

Merge /*+ APPEND */ Into Sas_Prod_Complete Tgt 
Using new_wkly_eop Src On 

( Src.Site_Id = Tgt.Site_Id And 
Src.Sku_Key = Tgt.Sku_Key And 
Src.Merchandising_Year = Tgt.Merchandising_Year And 
Src.Merchandising_Week = Tgt.Merchandising_Week) 
When Matched Then 

Update 

Set Tgt.Retail_On_Week = Src.Retail_On_Week,
Tgt.inventory_On_Week = Src.inventory_On_Week 
When Not Matched Then 

Insert ( Tgt.Site_Id, Tgt.Sku_Key, Tgt.Merchandising_Year, Tgt.Merchandising_Week, 
Tgt.Inventory_On_Week, Tgt.Retail_On_Week, Tgt.Cost_On_Week, 
Tgt.Sales_Retail, Tgt.Sales_Cost, Tgt.Sales_Qty, 
Tgt.Receipts_Retail, Tgt.Receipts_Cost, Tgt.Receipts_Qty, 
Tgt.Returns_Retail, Tgt.Returns_Cost, Tgt.Returns_Qty, 
Tgt.Transfers_Retail, Tgt.Transfers_Cost, Tgt.Transfers_Qty, 
Tgt.Mark_Down_Perm, Tgt.Mark_Up_Perm, Tgt.Mark_Down_Pos, Tgt.Mark_Up_Pos,
Tgt.Prev_Inv_On_Week, Tgt.Prev_Retail_On_Week, Tgt.Prev_Cost_On_Week, Tgt.Sales_Sold_Price,Tgt.Inv_Qty_Wkly,
Tgt.Adjustments_Retail, Tgt.Adjustments_Cost, Tgt.Adjustments_Qty )
Values ( Src.Site_Id, Src.Sku_Key, Src.Merchandising_Year, Src.Merchandising_Week, 
src.inventory_on_week, Src.Retail_On_Week, 0,
0,0,0, 

0,0,0, 

0,0,0, 

0,0,0, 

0,0,0,0, 

0,0,0,0,0,

0,0,0); 

Commit; 



Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 240 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_WKLY_MERGE_BOP_RETAIL();', Sysdate, Null);
Commit;


END P_WKLY_MERGE_EOP_RETAIL;

/
