--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_END
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_END" 
Is 
                        
                           
P_Process_Id Number;


Parm_Start_Week Number;

Parm_Start_Year Number;


Parm_End_Week Number;

Parm_End_Year Number;


Jobno Binary_Integer;


V_Where  Varchar2(500);


V_Where2  Varchar2(500);


Begin

/*Step 8.0 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

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


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 260,'Step 26:Create Sas_Prod_Complete Keys for Faster Reads.. PROCESS COMPLETED:p_wkly_merge_end',Sysdate,'sas_prod_complete','I');
Commit;


/*

Execute Immediate 'Create Unique Index Wkly_Sas_Complete_Pk 
    On Sas_Prod_Complete(Site_Id,Sku_Key, Merchandising_Year,Merchandising_Week )
TABLESPACE ERICDATA COMPRESS';

Execute Immediate 'Create Bitmap Index Wkly_Sas_Complete_Fk 
    On Sas_Prod_Complete(Merchandising_Year,Merchandising_Week) Local Nologging
Tablespace Ericdata Parallel (Degree 12 Instances Default)';
*/



If Parm_Start_Year = Parm_End_Year
Then

V_Where := ' Where 

    base.Merchandising_Year = '||Parm_Start_Year||' and
    base.Merchandising_Week Between '||Parm_Start_Week||' And '||Parm_End_Week;
Else 

V_Where := ' Where 

  ((base.Merchandising_Year = '||Parm_Start_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_Start_Year||')) Or
  (base.Merchandising_Year = '||Parm_End_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_End_Year||'))) '; 
End If;



If Parm_Start_Year = Parm_End_Year
Then

V_Where2 := ' and 

    Merchandising_Year = '||Parm_Start_Year||' and
    Merchandising_Week Between '||Parm_Start_Week||' And '||Parm_End_Week;
Else 

V_Where2 := ' and

  ((Merchandising_Year = '||Parm_Start_Year||' And
   Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_Start_Year||')) Or
  (Merchandising_Year = '||Parm_End_Year||' And
   Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_End_Year||'))) '; 
End If;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 260 And Process_Id = P_Process_Id; 
Commit;



Update Sas_Process_Log_id Set Process_Ind = 'C'
Where Process_Id = P_Process_Id; 
Commit;







Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 270,'Step 27:Archiving sas_prod_complete:p_wkly_merge_end',Sysdate,'sas_prod_complete','I');
Commit;

/*

Execute Immediate 'drop index Sas_Complete_2012Pk';
Execute Immediate 'drop index Sas_Complete_2012Fk';
*/


execute immediate 'Delete Sas_Prod_Complete_2012 base ' || v_where;
Commit;


execute immediate '

Insert  /*+ Append */  Into Sas_Prod_Complete_2012 
(SITE_ID, Sku_Key, Merchandising_Year, 
Merchandising_Week, Inventory_On_Week,  Retail_On_Week,     Cost_On_Week,       
Sales_Qty,          Sales_Retail,     Sales_Cost,         Receipts_Qty,       
Receipts_Retail,    Receipts_Cost,    Returns_Qty,        Returns_Retail,     
Returns_Cost,       Transfers_Qty,      Transfers_Retail,   Transfers_Cost,     
Mark_Down_Perm,     Mark_Up_Perm,       Mark_Down_Pos,      Mark_Up_Pos,        
Prev_Inv_On_Week,   Prev_Retail_On_Week, Prev_Cost_On_Week,  Sales_Sold_Price, Inv_Qty_Wkly,
Adjustments_Retail, Adjustments_Cost,    Adjustments_Qty
)   

Select

Site_Id,            Sku_Key,          Merchandising_Year, Merchandising_Week, 
Nvl(Inventory_On_Week,0),  Nvl(Retail_On_Week,0),     Nvl(Cost_On_Week,0),       Nvl(Sales_Qty,0),          
Nvl(Sales_Retail,0),       Nvl(Sales_Cost,0),         Nvl(Receipts_Qty,0),       Nvl(Receipts_Retail,0),    
Nvl(Receipts_Cost,0),      Nvl(Returns_Qty,0),        Nvl(Returns_Retail,0),     Nvl(Returns_Cost,0),       
Nvl(Transfers_Qty,0),      Nvl(Transfers_Retail,0),   Nvl(Transfers_Cost,0),     Nvl(Mark_Down_Perm,0),     
Nvl(Mark_Up_Perm,0),       Nvl(Mark_Down_Pos,0),      Nvl(Mark_Up_Pos,0),        Nvl(Prev_Inv_On_Week,0),   
Nvl(Prev_Retail_On_Week,0),Nvl(Prev_Cost_On_Week,0),  Nvl(Sales_Sold_Price,0),   Nvl(Inv_Qty_Wkly,0),
nvl(Adjustments_Retail,0) ,nvl(Adjustments_cost,0),   nvl(Adjustments_Qty,0)
From               

Sas_Prod_Complete Base ' || v_where;
Commit;


execute immediate '

update sas_prod_complete_2012 base
set retail_on_week = 0, cost_on_week = 0 Where inventory_on_week = 0 and merchandising_year=' || Parm_Start_Year; 


execute immediate '

update sas_prod_complete_2012 base
set prev_retail_on_week = 0, prev_cost_on_week = 0  Where prev_inv_on_week = 0 and merchandising_year=' || Parm_Start_Year;

commit;


execute immediate '

delete sas_prod_complete_2012 where
sales_qty = 0

and receipts_qty = 0
and returns_qty = 0 
and transfers_qty = 0 
and inv_qty_wkly = 0
and inventory_on_week = 0
and  mark_down_perm = 0
and mark_up_perm = 0
and mark_down_pos = 0 
and sales_sold_price = 0
and prev_inv_on_week = 0
and retail_on_week = 0 
and merchandising_year=' || Parm_Start_Year;

Update Sas_Process_Sw 
Set Process_Time = Sysdate, Process_Complete = 'true';
commit;

execute immediate 'Delete Inv_Move_arc base ' || v_where;
Commit;


execute immediate '

INSERT  /*+ Append */

  INTO INV_MOVE_arc

    (
      MERCHANDISING_WEEK , INVEN_MOVE_DATE ,  RETAIL_PRICE_FINAL ,
      STYLE_ID , SITE_ID , MERCHANDISING_YEAR , SECTION_ID ,
      RETAIL_PRICE , INVEN_MOVE_QTY , INVEN_MOVE_TYPE ,
      AVERAGE_COST ,  SIZE_ID ,  LANDED_UNIT_COST ,  DIMENSION_ID ,
      Color_Id
    )
Select     
MERCHANDISING_WEEK , INVEN_MOVE_DATE , RETAIL_PRICE_FINAL , STYLE_ID ,
SITE_ID , MERCHANDISING_YEAR , SECTION_ID , RETAIL_PRICE ,INVEN_MOVE_QTY ,
INVEN_MOVE_TYPE , AVERAGE_COST , SIZE_ID , LANDED_UNIT_COST , Dimension_Id ,
Color_Id
From 
Wkly_Inv_Move_Extract Base ' || v_where;
Commit;

execute immediate 'Delete NEW_WKLY_EOP_2012 base ' || v_where;
Commit;

execute immediate '

  Insert
  INTO NEW_WKLY_EOP_2012
    (
      MERCHANDISING_WEEK ,
      SKU_KEY ,
      RETAIL_ON_WEEK ,
      INVENTORY_ON_WEEK ,
      MERCHANDISING_YEAR ,
      SITE_ID
    )
  select 
      MERCHANDISING_WEEK ,
      SKU_KEY ,
      RETAIL_ON_WEEK ,
      INVENTORY_ON_WEEK ,
      MERCHANDISING_YEAR ,
      Site_Id
  from New_Wkly_Eop base ' || v_where;
Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 270 And Process_Id = P_Process_Id; 
Commit;


/*

Execute Immediate 'Create Unique Index Sas_Complete_2012Pk 
    On Sas_Prod_Complete_2012(Site_Id,Sku_Key, Merchandising_Year,Merchandising_Week )
TABLESPACE ERICDATA COMPRESS';

Execute Immediate 'Create Bitmap Index Sas_Complete_2012Fk 
    On Sas_Prod_Complete_2012(Merchandising_Year,Merchandising_Week) Local Nologging
Tablespace Ericdata Parallel (Degree 12 Instances Default)';
*/


--update sas_process_calendar 
--set processed = 'Y', Processed_Date = sysdate
--where exists (select 1 from Wkly_Sas_Prod_Time_Pd wp where wp.merch_year = sas_process_calendar.merchandising_year 
--            and wp.merch_week = sas_process_calendar.merchandising_week);  
--commit;
--
--Dbms_Job.Submit(Jobno, 'P_WKLY_TIME_PERIOD_Sp();', Sysdate, Null);
--commit;



END P_WKLY_Merge_end;

/
