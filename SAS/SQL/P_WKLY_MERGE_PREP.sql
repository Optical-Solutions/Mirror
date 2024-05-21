--------------------------------------------------------
--  DDL for Procedure P_WKLY_MERGE_PREP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MERGE_PREP" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 8.0 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 100,'Step 10:Merge Prep and truncate Sas_Prod_Complete:p_wkly_merge_prep',Sysdate,'sas_prod_complete','I');
Commit;


--Execute Immediate 'drop index WKLY_SAS_COMPLETE_PK';
--Execute Immediate 'drop index WKLY_SAS_COMPLETE_FK';
/* truncate sas_prod_complete to avoid mutliple occurrence of MarkDown*/
Execute Immediate 'truncate table sas_prod_complete';


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 100 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_merge_xfer();', Sysdate, Null);
Commit;


END P_WKLY_Merge_prep;      

/
