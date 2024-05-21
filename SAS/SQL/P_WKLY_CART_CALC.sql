--------------------------------------------------------
--  DDL for Procedure P_WKLY_CART_CALC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_CART_CALC" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*

Process is part of the original way we use to calculate BOP/EOP.

This way will only work for COST and is still used just for cost.

Since it has been learned that to get the correct EOP retail you need to 
follow a special RIM formula.

*/


Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id Where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log 
(Process_Id,Process_Step,Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values 

(P_Process_Id,70,'Step 7:Process Cartesian table to get Cost:p_wkly_cart_calc',Sysdate,'wkly_tmp_sas_inventory_calc','I');
Commit;



 Dbms_Stats.Gather_Table_Stats(Ownname => 'ERIC',Tabname => 'Wkly_Tmp_Sas_Inventory_Calc');


For Asitegroup In (Select Distinct Site_Group From Wkly_Sas_Sites
                      Where Processed_Ind Is Null Order By Site_Group)
Loop


    Execute Immediate 'DROP table wkly_tmp_sas_prod_inv_cart ';
    Execute Immediate '

        CREATE TABLE wkly_tmp_sas_prod_inv_cart 
        (

        site_id VARCHAR2(5) NOT NULL,
        sku_key VARCHAR2(25) NOT NULL,
        merch_year NUMBER(4) NOT NULL,
        merch_week NUMBER(2) NOT NULL
        )

        PARALLEL 12 COMPRESS TABLESPACE ERICDATA
    ';


--Execute Immediate 'create index wtspic_idx0 on wkly_tmp_sas_prod_inv_cart(site_id, sku_key, merch_year, merch_week)';

  Update Wkly_Sas_Sites 

     Set Start_Date = Sysdate 
  Where Site_Group = Asitegroup.Site_Group;
  Commit;


    Insert /*+ APPEND */ Into Wkly_Tmp_Sas_Prod_Inv_Cart
    (Site_Id,Sku_Key,Merch_Year, Merch_Week)
    (

        Select Site_Id, Sku_Key, Merch_Year, Merch_Week
        From (Select Site_Id 

              From Wkly_Sas_Sites 
              Where Site_Group = Asitegroup.Site_Group
             ),

             (Select Sku_Key 

              From Sas_Product_Master Ma 
                   Join Wkly_Sas_Styles St 
                   On (Ma.Style_Id = St.Style_Id)
             ), 

             /* verify weeks to make sure all weeks are zero filled **Remember
                we add  min week minus 1 */
             (Select Merch_Year, Merch_Week 
              From Wkly_Sas_Prod_Time_Pd
             )

    );

    Commit;


Merge /*+ APPEND */ Into Wkly_Tmp_Sas_Inventory_Calc Tcalc
    Using (

        Select M.Site_Id, M.Sku_Key, M.Merch_Year, M.Merch_Week
        From Wkly_Tmp_Sas_Prod_Inv_Cart M
    ) Cart

        On (    Tcalc.Site_Id = Cart.Site_Id 
            And Tcalc.Sku_Key = Cart.Sku_Key 
            And Tcalc.Merchandising_Year = Cart.Merch_Year 
            And Tcalc.Merchandising_Week = Cart.Merch_Week)
    When Not Matched Then

        Insert

        (Tcalc.Site_Id,Tcalc.Sku_Key,Tcalc.Inven_Move_Qty,Tcalc.Retail_Price,
     Tcalc.Cost,Tcalc.Merchandising_Year,Tcalc.Merchandising_Week)
        Values (Cart.Site_Id,Cart.Sku_Key,0,0,0,Cart.Merch_Year,Cart.Merch_Week);

    Update Wkly_Sas_Sites 

     Set Processed_Ind = '1', 
           Processed_Date = Sysdate 
     Where Site_Group = Asitegroup.Site_Group;
    Commit;


End Loop;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 70 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_sas_inv();', Sysdate, Null);
--Dbms_Job.Submit(Jobno, 'P_Wkly_sas_inv_sp();', Sysdate, Null);
Commit;



End P_Wkly_Cart_Calc;

/
