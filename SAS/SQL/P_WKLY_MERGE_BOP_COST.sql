--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_BOP_COST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_BOP_COST" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

max_week Number;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 230,'Step 23:Merge/Insert Process BOP Cost:p_wkly_merge_bop',Sysdate,'sas_prod_complete','I');
Commit;



--wkly_sas_inv

For Rec In (Select w.Merchandising_Week, w.Merchandising_Year From Wkly_Sas_Inv w
            Group By Merchandising_Year, Merchandising_Week Order By Merchandising_Year, Merchandising_Week)
Loop

Select max(M.Merchandising_Week) Into Max_Week From Merchandising_Calendars M 
where m.merchandising_year = rec.merchandising_year;


Merge  /*+ APPEND */

Into Sas_Prod_Complete Tgt      
Using (Select * From Wkly_Sas_Inv Wsi 
       Where Wsi.Merchandising_Week = Rec.Merchandising_Week And 
             Wsi.Merchandising_Year = rec.merchandising_year) Src  
On ( Src.Site_Id = Tgt.Site_Id And
     Src.Sku_Key = Tgt.Sku_Key And
    (Case Src.Merchandising_Week When Max_Week
       Then (Src.Merchandising_Year + 1)
       Else Src.Merchandising_Year End) = Tgt.Merchandising_Year And
   (Case Src.Merchandising_Week When Max_week
      Then 1

      Else (Src.Merchandising_Week + 1) end) = Tgt.Merchandising_Week )
WHEN MATCHED

THEN

UPDATE

Set 

Tgt.Prev_Cost_On_Week = Src.Cost_On_Week
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
Tgt.Prev_Inv_On_Week, Tgt.Prev_Retail_On_Week, Tgt.Prev_Cost_On_Week, Tgt.Sales_Sold_Price,Tgt.Inv_Qty_Wkly,
Tgt.Adjustments_Retail, Tgt.Adjustments_Cost, Tgt.Adjustments_Qty
)

Values ( 

Src.Site_Id, Src.Sku_Key, 

(Case Src.Merchandising_Week When (Select Max(Merchandising_Week) From Merchandising_Calendars
                                       where Merchandising_Year = Src.Merchandising_Year)
  Then (Src.Merchandising_Year + 1)
  Else Src.Merchandising_Year End), 
(Case Src.Merchandising_Week When (Select Max(Merchandising_Week) From Merchandising_Calendars
                                       where Merchandising_Year = Src.Merchandising_Year) 
 Then 1

 Else (Src.Merchandising_Week + 1) End),
       0,0,0, 

       0,0,0,   

       0,0,0,  

       0,0,0,  

       0,0,0,   

       0,0,0,0,

       0, 0, Src.Cost_On_Week, 0,0,
       0,0,0);

Commit;

End Loop;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 230 And Process_Id = P_Process_Id; 
commit;



Dbms_Job.Submit(Jobno, 'P_WKLY_MERGE_EOP_RETAIL();', Sysdate, Null);
commit;


END P_WKLY_MERGE_BOP_COST;  

/
