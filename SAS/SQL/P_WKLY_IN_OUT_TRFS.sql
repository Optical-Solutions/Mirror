--------------------------------------------------------
--  DDL for Procedure P_WKLY_IN_OUT_TRFS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_IN_OUT_TRFS" 
Is 
                        
                           
P_Process_Id Number;


Parm_Start_Week Number;

Parm_Start_Year Number;


Parm_End_Week Number;

Parm_End_Year Number;


Jobno Binary_Integer;


V_Where_Group  Varchar2(1000);
D_Where        Varchar2(500);
Trf            Varchar2(9) := 'TRANSFERS';

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


If Parm_Start_Year = Parm_End_Year
Then

D_Where := ' Where 

    base.Merchandising_Year = '||Parm_Start_Year||' and
    base.Merchandising_Week Between '||Parm_Start_Week||' And '||Parm_End_Week;
Else 

D_Where := ' Where 

  ((base.Merchandising_Year = '||Parm_Start_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_Start_Year||')) Or
  (base.Merchandising_Year = '||Parm_End_Year||' And
   base.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
                            Where W.Merch_Year = '||Parm_End_Year||'))) '; 
End If;


V_Where_Group := ' Where inven_move_type = ' || '''' || Trf || '''';
V_Where_Group :=  V_Where_Group || 'group by site_id,sku_key,merchandising_year,merchandising_week';


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 31,'Step 31:Delete weeks sas_transfers:P_WKLY_IN_OUT_TRFS',Sysdate,'sas_prod_complete','I');
Commit;


execute immediate 'Delete sas_transfers base ' || D_where;
Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 31 And Process_Id = P_Process_Id; 
Commit;


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 32,'Step 32:Insert new sas_transfers..:P_WKLY_IN_OUT_TRFS',Sysdate,'sas_prod_complete','I');
Commit;


execute immediate 'insert into sas_transfers
select wkt.site_id,sku_key,wkt.merchandising_year,
wkt.merchandising_week,

sum(case when inven_move_qty > 0 then inven_move_qty else 0 end) transfer_qty_in,
sum(case when inven_move_qty > 0 then inven_move_qty*retail_price else 0 end) transfer_retail_in,
sum(case when inven_move_qty > 0 then inven_move_qty*landed_unit_cost else 0 end) transfer_cost_in,
sum(case when inven_move_qty < 0 then inven_move_qty else 0 end) transfer_qty_out,
sum(case when inven_move_qty < 0 then inven_move_qty*retail_price else 0 end) transfer_retail_out,
sum(case when inven_move_qty < 0 then inven_move_qty*landed_unit_cost else 0 end) transfer_cost_out
from wkly_inv_move_extract wkt
Join (Select Style_Id, Min(Sku_Key) Sku_Key 
            From Sas_Product_Master
            Group By Style_Id) Spg1 On (wkt.Style_Id = Spg1.Style_Id)' || V_Where_Group;
Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 32 And Process_Id = P_Process_Id; 
Commit;



END P_WKLY_IN_OUT_TRFS;     

/
