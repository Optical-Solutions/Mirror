--------------------------------------------------------
--  DDL for Procedure P_WKLY_SUM_CART
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_SUM_CART" 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;



Begin

/*Step 7*/


Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,190,'Step 19:Prepare Cartesian Table for Retail/Quantity:p_wkly_sum_cart',Sysdate,'wkly_tmp_sas_inventory_calc','I');
commit;


Execute Immediate 'drop table wkly_tmp_Sum_Calc' ;
execute immediate q'[ Create Table wkly_tmp_Sum_Calc     
           Compress Parallel 12      Tablespace Ericdata Nologging     
           Partition By Range(Merchandising_Week)     
           Interval (1)     ( Partition P1 Values Less Than (2) ) As     
          Select 

            M.Site_Id, 

            M.Sku_Key, 

            Merchandising_Year, 
            Merchandising_Week,      
            MARK_DOWN_PERM, 
            MARK_UP_PERM, 

            MARK_DOWN_POS, 
            (Round((Nvl(Sales_Sold_Price,0) * -1) * (nvl(Shrinkage_Rate,0) / 100),2) ) Sales_Shrink,
            (Sales_Sold_Price * -1) sales_sold_price,
            (receipts_retail - (returns_retail * -1)) purchases,
            transfers_retail,
            inv_qty_wkly inven_qty,
            adjustments_retail
           From            
              Sas_Prod_Complete M     


           Join Sas_Product_Master Sm On (Sm.Sku_Key = M.Sku_Key) 
           left Join Styles Sty On (sty.business_unit_id = '30' and Sty.Style_Id = Sm.Style_Id)  
           left Join Sections Sec On (Sec.Section_Id = Sty.Section_Id and sty.business_unit_id = sec.business_unit_id)]';




execute Immediate 'Truncate Table wkly_sum_sites';

insert into wkly_sum_sites

    SELECT site_id, mod(rownum,30) site_group , 
           '0' processed_ind, 
           to_date('19000101','YYYYMMDD') processed_date, 
           to_date('19000101','YYYYMMDD') start_date
    FROM (

            Select Distinct Site_Id 
            FROM wkly_tmp_Sum_Calc
            ORDER BY 1

         );

commit;


update wkly_sum_sites 

   set processed_ind = null, 
       processed_date = null, 
       Start_Date = Null;

commit;



Execute Immediate 'Truncate Table Wkly_sum_Styles';
Insert Into Wkly_sum_Styles
Select Distinct Style_Id From wkly_tmp_Sum_Calc I Join Sas_Product_Master S On (S.Sku_Key = I.Sku_Key);
commit;


Dbms_Stats.Gather_Table_Stats(Ownname => 'ERIC',tabname => 'wkly_tmp_sum_calc');


Update wkly_sum_sites Set Processed_Ind = Null, Processed_Date = Null, Start_Date = Null; 
  Commit; 



For Asitegroup In (Select Distinct Site_Group From Wkly_Sum_Sites
                      WHERE processed_ind IS NULL ORDER BY site_group)
LOOP


    execute immediate 'DROP table wkly_tmp_sum_cart ';
    Execute Immediate '

        CREATE TABLE wkly_tmp_sum_cart 
        (

        site_id VARCHAR2(5) NOT NULL,
        sku_key VARCHAR2(25) NOT NULL,
        merch_year NUMBER(4) NOT NULL,
        merch_week NUMBER(2) NOT NULL
        )

        PARALLEL 12 COMPRESS TABLESPACE ERICDATA
    ';


  UPDATE wkly_sum_Sites 

     SET start_date = sysdate 
  WHERE site_group = asitegroup.site_group;
  commit;


    INSERT /*+ APPEND */ INTO wkly_tmp_sum_cart
    (site_id,sku_key,merch_year, merch_week)
    (

        SELECT site_id, sku_key, merch_year, merch_week
        From (Select Site_Id 

              FROM wkly_sum_Sites 
              WHERE site_group = asitegroup.site_group
             ),

            (Select Sku_Key 

              From Sas_Product_Master Ma 
                   JOIN Wkly_Sum_Styles st 
                   ON (ma.style_id = st.style_id)
             ), 

             (Select Merch_Year, Merch_Week 
              FROM wkly_sas_prod_time_pd
             )

    );

    commit;


MERGE /*+ APPEND */ INTO wkly_tmp_sum_calc tcalc
    USING (

        Select M.Site_Id, M.Sku_Key, M.Merch_Year, M.Merch_Week
        FROM wkly_tmp_sum_cart m

    ) cart

        ON (    tcalc.site_id = cart.site_id 
            AND tcalc.sku_key = cart.sku_key 
            AND tcalc.merchandising_year = cart.merch_year 
            AND tcalc.merchandising_week = cart.merch_week)
    WHEN NOT MATCHED THEN

        Insert

        (Tcalc.Site_Id,Tcalc.Sku_Key,Tcalc.Mark_Down_Perm,Tcalc.Mark_Up_Perm,Tcalc.Mark_Down_Pos,Tcalc.Sales_Shrink,Tcalc.Sales_Sold_Price,Tcalc.Purchases,Tcalc.Transfers_Retail,Tcalc.Inven_Qty,Tcalc.Adjustments_Retail, Tcalc.Merchandising_Year,Tcalc.Merchandising_Week)
        VALUES (cart.site_id,cart.sku_key,0,0,0,0,0,0,0,0,0,cart.merch_year,cart.merch_week);

    Update Wkly_sum_Sites 

     SET processed_ind = '1', 
           processed_date = sysdate 
     WHERE site_group = asitegroup.site_group;
    commit;


End Loop;



Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 190 And Process_Id = P_Process_Id; 
Commit;


Dbms_Stats.Gather_Table_Stats(Ownname => 'ERIC',tabname => 'wkly_tmp_sum_calc');


Dbms_Job.Submit(Jobno, 'P_Wkly_sum_total();', Sysdate, Null);
Commit;


END P_WKLY_SUM_CART;

/
