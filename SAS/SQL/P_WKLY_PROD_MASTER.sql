--------------------------------------------------------
--  DDL for Procedure P_WKLY_PROD_MASTER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_PROD_MASTER" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 1.3 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,30,'Step 3:Update Sas_Prod_Master from MC2P:p_wkly_prod_master',Sysdate,'sas_product_master;sas_product_master_sc','I');
Commit;


execute Immediate 'drop table sas_product_master';
execute immediate 'drop table sas_product_master_sc';
execute immediate 'drop table v_dept_class_subclass';
execute immediate 'drop table styles';
--execute immediate 'drop materialized view v_dept_class_subclass';
--execute immediate 'drop materialized view styles';

Execute Immediate 'Create Table Sas_Product_Master As Select * From Sas_Product_Master @Mc2p';
execute immediate 'Create Table Sas_Product_Master_sc As Select * From Sas_Product_Master_sc @Mc2p';
execute immediate q'[Create Table v_dept_class_subclass As Select * From v_dept_class_subclass @Mc2p where business_unit_id = '30']';
execute immediate 'Create Table STYLES As Select * From STYLES @Mc2p';
--execute immediate 'create materialized view v_dept_class_subclass REFRESH COMPLETE as select * from v_dept_class_subclass@MC2R';
--execute immediate 'create materialized view STYLES REFRESH COMPLETE as select * from STYLES@MC2P';


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 30 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_im_group();', Sysdate, Null);
Dbms_Job.Submit(Jobno, 'P_Wkly_im_group_types();', Sysdate, Null);
Commit;


END P_WKLY_PROD_MASTER;     

/
