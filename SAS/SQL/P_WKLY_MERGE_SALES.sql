--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_SALES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_SALES" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 150,'Step 15:Merge/Insert Process Sales:p_wkly_merge_sales',Sysdate,'sas_prod_complete','I');
Commit;


Merge /*+ APPEND */

Into Sas_Prod_complete Tgt

Using wkly_im_sas_sales Src
On ( Src.Site_Id = Tgt.Site_Id And
Src.Sku_Key = Tgt.Sku_Key And
Src.Merchandising_Year = Tgt.Merchandising_Year And
src.merchandising_week = tgt.merchandising_week)
WHEN MATCHED

THEN

UPDATE

Set 

Tgt.Sales_Retail = Src.Retail_Price,
Tgt.Sales_sold_price = Src.Retail_Price_v2,
Tgt.Sales_Cost = Src.Cost,

Tgt.sales_qty = src.inven_move_qty
WHEN NOT MATCHED

Then

Insert ( 

Tgt.Site_Id, Tgt.Sku_Key, Tgt.Merchandising_Year, Tgt.Merchandising_Week,
Tgt.Inventory_On_Week, Tgt.Retail_On_Week, Tgt.Cost_On_Week,
Tgt.Sales_Retail, Tgt.Sales_Cost, Tgt.Sales_Qty,
Tgt.Receipts_Retail, Tgt.Receipts_Cost, Tgt.Receipts_Qty,
Tgt.Returns_Retail, Tgt.Returns_Cost, Tgt.Returns_Qty,
Tgt.Transfers_Retail, Tgt.Transfers_Cost, Tgt.Transfers_Qty,
Tgt.Mark_Down_Perm, Tgt.Mark_Up_Perm, Tgt.Mark_Down_Pos, Tgt.Mark_Up_Pos,
     Tgt.Prev_Inv_On_Week, Tgt.Prev_Retail_On_Week, Tgt.Prev_Cost_On_Week,Tgt.Sales_Sold_Price,Tgt.Inv_Qty_Wkly,
Tgt.Adjustments_Retail, Tgt.Adjustments_Cost, Tgt.Adjustments_Qty
)

Values ( 

Src.Site_Id, Src.Sku_Key, Src.Merchandising_Year, Src.Merchandising_Week,
0,0,0, 

       Src.Retail_Price, Src.Cost, Src.Inven_Move_Qty,   
       0,0,0,   

       0,0,0,  

       0,0,0,        

       0,0,0,0,

      0,0,0,Src.Retail_Price_V2,0,
      0,0,0);

Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 150 And Process_Id = P_Process_Id; 
commit;



Dbms_Job.Submit(Jobno, 'P_Wkly_merge_md();', Sysdate, Null);
Commit;



END P_WKLY_merge_sales;     

/
