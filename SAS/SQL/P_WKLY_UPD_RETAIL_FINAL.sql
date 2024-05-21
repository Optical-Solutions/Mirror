--------------------------------------------------------
--  DDL for Procedure P_WKLY_UPD_RETAIL_FINAL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_UPD_RETAIL_FINAL" 
Is 
                        
/*Step 3*/


P_Process_Id Number;

Jobno Binary_Integer;


Begin

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,030,'0030 Process Sales Type - Get Perm Price (Go Fish)',Sysdate,'wkly_inv_move_extract','I');
Commit;



--Execute Immediate 'drop table prices';
--Execute Immediate q'[Create Table Prices As Select * From prices@Mc2p where PRICE_SUB_TYPE = 'PERM']';

 /*

Update Wkly_Inv_Move_Extract
Set Retail_Price_Final = Z_Get_Price_Check('30', Site_Id, Style_Id, Color_Id, Dimension_Id, Size_Id, Inven_Move_Date, Retail_Price)
Where Inven_Move_Type = 'SALES';
Commit;*/


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 030 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_prod_master();', Sysdate, Null);
Dbms_Job.Submit(Jobno, 'P_Wkly_im_group();', Sysdate, Null);
Dbms_Job.Submit(Jobno, 'P_Wkly_im_group_types();', Sysdate, Null);
Commit;



END P_WKLY_UPD_RETAIL_FINAL;

/
