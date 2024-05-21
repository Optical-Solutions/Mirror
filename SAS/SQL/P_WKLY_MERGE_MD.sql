--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_MD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_MD" 
Is 
                        
                           
P_Process_Id Number;


Parm_Start_Week Number;

Parm_Start_Year Number;


Parm_End_Week Number;

Parm_End_Year Number;


Jobno Binary_Integer;


V_Where  Varchar2(500);





Begin

/*Step  */


Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id Where Process_Type = 'WEEKLY'; 

/*Get Ending week */

Select 

  Merch_Week, Merch_Year Into Parm_End_Week, Parm_End_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;

/*Get Starting Week*/

Select 

  Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;


If Parm_Start_Year = Parm_End_Year
Then

V_Where := '  

    base.Merchandising_Year = '||Parm_Start_Year||' and
    base.Merchandising_Week Between '||Parm_Start_Week||' And '||Parm_End_Week;
Else 

V_Where := '  

  ((base.Merchandising_Year = '||Parm_Start_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_Start_Year||')) Or
  (base.Merchandising_Year = '||Parm_End_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_End_Year||'))) '; 
End If;


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,160,'Step 16:Merge/Insert Markdown Data:p_wkly_merge_md',Sysdate,'sas_prod_complete','I');
Commit;


execute immediate '

Merge  /*+ APPEND */

Into Sas_Prod_Complete Tgt

Using (Select base.Site_Id, base.Style_Id, base.Merchandising_Year, base.Merchandising_Week, 
        sum(base.Mark_Down_Perm) Mark_Down_Perm, sum(base.Mark_Up_Perm) Mark_Up_Perm, sum(base.Mark_Down_Pos) Mark_Down_Pos,
        Spl.Sku_Key 

       From Mark_Down_Collection_All base 
       Join (select style_id, min(sku_key) sku_key from sas_product_master group by style_id) Spl On (base.Style_Id = Spl.Style_Id )
        Where 

         (base.Mark_Down_Perm <> 0 Or 
         base.Mark_Up_Perm <> 0 Or 
         base.Mark_Down_Pos <> 0) And ' || v_where ||
         ' 

         group by

base.Site_Id, base.Style_Id, base.Merchandising_Year, base.Merchandising_Week,  Spl.Sku_Key ) Src
On (Src.Site_Id = Tgt.Site_Id And
Src.Sku_Key = Tgt.Sku_Key And
Src.Merchandising_Year = Tgt.Merchandising_Year And
src.merchandising_week = tgt.merchandising_week)
WHEN MATCHED

THEN

UPDATE

Set 

Tgt.Mark_Down_Perm = Src.Mark_Down_Perm,
Tgt.Mark_Up_Perm = Src.Mark_Up_Perm,
Tgt.Mark_Down_Pos = Src.Mark_Down_Pos
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

0,0,0,   

0,0,0,   

0,0,0,  

0,0,0, 

Src.Mark_Down_Perm, Src.Mark_Up_Perm, Src.Mark_Down_Pos,0,
0,0,0,0,0,

0,0,0)';

Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 160 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_merge_wkly_qty();', Sysdate, Null);
Commit;



END P_WKLY_MERGE_MD;        

/
