--------------------------------------------------------
--  DDL for Procedure P_WKLY_TYPES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_TYPES" 
Is 
                        
                           
P_Process_Id Number;

P_Type Varchar(20);

Jobno Binary_Integer;


Begin

/*

This Procedure is used to create the table needed for the individual
transaction totals.


Sales, Receiving, Transfers, Returns, Adjustment.  

The numbers will be merged/inserted into sas_prod_complete and
sent to SAS.


These totals will also be used in the BOP/EOP Calculation
*/



Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id 

where Process_Type = 'WEEKLY'; 


Insert Into Sas_Process_Log 
  (Process_Id, Process_Step, Process_Name,
   Process_Start_Time,Process_Table,Process_Ind)
Values 

  (P_Process_Id, 80, 'Step 8:Create Table for each move type in IM_Group_types:p_wkly_types',
   Sysdate,'Review Procedure - Multiple Tables','I');
Commit;


---Receipts  Starts----

Execute Immediate 'drop table wkly_im_sas_receipts';
execute Immediate 'CREATE TABLE wkly_im_sas_receipts
    COMPRESS TABLESPACE ERICDATA
    PARTITION BY RANGE(merchandising_week)
    INTERVAL (1)    

    (

        PARTITION p1 VALUES LESS THAN (2)
    ) AS

    Select Site_Id, Sku_Key, Inven_Move_Qty, 
           Retail_Price, Cost, Merchandising_Year, Merchandising_Week
    FROM wkly_Im_Sas_Grp_Types
    WHERE inven_move_type = ''RECEIVING''';

---Transfers Starts----

Execute Immediate 'drop table wkly_im_sas_transfers';
Execute Immediate 'CREATE TABLE wkly_im_sas_transfers
    COMPRESS TABLESPACE ERICDATA
    PARTITION BY RANGE(merchandising_week)
    INTERVAL (1)    

    (

        PARTITION p1 VALUES LESS THAN (2)
    ) AS

    Select Site_Id, Sku_Key, Inven_Move_Qty, 
           Retail_Price, Cost, Merchandising_Year, Merchandising_Week
    From Wkly_Im_Sas_Grp_Types
    WHERE inven_move_type = ''TRANSFERS''';


-- Claims/Returns starts --
Execute Immediate 'drop table wkly_im_sas_claims';
Execute Immediate 'CREATE TABLE wkly_im_sas_claims
    COMPRESS TABLESPACE ERICDATA
    PARTITION BY RANGE(merchandising_week)
    INTERVAL (1)    

    (

        PARTITION p1 VALUES LESS THAN (2)
    ) AS

    Select Site_Id, Sku_Key, Inven_Move_Qty, 
           Retail_Price, Cost, Merchandising_Year, Merchandising_Week
    From Wkly_Im_Sas_Grp_Types
    WHERE inven_move_type =  ''RETURNS''';

-- Sales starts --

Execute Immediate 'drop table wkly_im_sas_sales';
Execute Immediate 'CREATE TABLE wkly_im_sas_sales
    COMPRESS TABLESPACE ERICDATA
    PARTITION BY RANGE(merchandising_week)
    INTERVAL (1)    

    (

        PARTITION p1 VALUES LESS THAN (2)
    ) AS

    Select Site_Id, Sku_Key, Inven_Move_Qty, Retail_Price, 
           Retail_Price_V2, Cost, Merchandising_Year, Merchandising_Week
    From wkly_Im_Sas_Grp_Types
    Where Inven_Move_Type = ''SALES''';

-- Adjustment starts --

Execute Immediate 'drop table wkly_im_sas_adjustments';
Execute Immediate 'CREATE TABLE wkly_im_sas_adjustments
    COMPRESS TABLESPACE ERICDATA
    PARTITION BY RANGE(merchandising_week)
    INTERVAL (1)    

    (

        PARTITION p1 VALUES LESS THAN (2)
    ) AS

    Select Site_Id, Sku_Key, Inven_Move_Qty, 
           Retail_Price, Cost, w.Merchandising_Year, w.Merchandising_Week
    From Wkly_Im_Sas_Grp_Types w
    WHERE inven_move_type =  ''ADJUSTMENT''';


    /* or

    (Inven_Move_Type =''PHYSICAL'' And
     12 = (Select Distinct M.Merchandising_Period From Merchandising_Calendars M
                           Where M.Merchandising_Year = W.Merchandising_Year And
                                 M.Merchandising_Week = w.Merchandising_Week)
     )

     */


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 80 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_mark_down();', Sysdate, Null);
Commit;


END P_WKLY_TYPES;           

/
