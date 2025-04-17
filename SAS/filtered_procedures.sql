create or replace procedure        P_WKLY_TIME_PERIOD 
(num_weeks in number) Is 
  
/*Step 1 */


P_Process_Id Number;

Jobno Binary_Integer;

Parm_Start_Week Number;

Parm_End_Week Number;

Begin


/* Clear Table to Insert New Weeks*/
Execute Immediate 'Truncate Table wkly_sas_prod_time_pd';

Update Sas_Process_Sw 

Set Process_Time = Sysdate, Process_Complete = 'false';
Commit;


Insert Into Sas_Process_Log_Id (Process_Ind,Process_Date,Process_Type) 
Values ('I',Sysdate,'WEEKLY');
Commit;



/* Get Process_id from Log */
Select 

  Max(Process_Id) Into P_Process_Id 
From 

  Sas_Process_Log_Id 

where Process_Type = 'WEEKLY'; 

/*Write to Log*/

Insert Into Sas_Process_Log 
    (Process_Id, Process_Step, Process_Name, 
     Process_Start_Time, Process_Table, Process_Ind)
Values 

    (P_Process_Id, 10, 'Step 1:Get weeks to process:p_wkly_time_period ', 
     Sysdate, 'Wkly_Sas_Prod_Time_Pd', 'I');
Commit;




/*Process/Insert Week*/

For Rec In 

 (Select Mc.Merchandising_Year, Mc.Merchandising_Week, Mc.Merchandising_Period
   From (Select * From Merchandising_Calendars
         Where 

           Business_Unit_Id = 30 And Week_Ending_Date <= Trunc(Sysdate) - 1
         Order By  

           Merchandising_Year Desc, Merchandising_Week Desc) Mc
   Where Rownum <= Nvl(Num_Weeks,1))
   --Where Rownum <= 6)

Loop

  Insert Into Wkly_Sas_Prod_Time_Pd
  Select rec.Merchandising_Year, rec.Merchandising_Week From Dual;
  Commit;

End Loop;


/*Get Ending week */

Select Merch_Week Into Parm_End_Week 
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;

/*Get Starting Week*/

Select Merch_Week Into Parm_Start_Week 
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate,  
                           process_name = 'Step 1:Get weeks to process:p_wkly_time_period: '||Parm_Start_Week|| ' thru ' ||Parm_End_Week
Where Process_Step = 10 And Process_Id = P_Process_Id; 
commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 10 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_Inventory_Extract();', Sysdate, Null);
commit;


END P_WKLY_TIME_PERIOD;


create or replace procedure        P_WKLY_INVENTORY_EXTRACT 
Is 
                        
/*Step 2 */


P_Process_Id Number;


Parm_Start_Week Number;

Parm_Start_Year Number;


Parm_End_Week Number;

Parm_End_Year Number;


Jobno Binary_Integer;


V_Where  Varchar2(500);



Begin



Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id Where Process_Type = 'WEEKLY';

/*Log Process*/

Insert Into Sas_Process_Log 
(Process_Id, Process_Step, Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values 

(P_Process_Id, 20,'Step 2:Extract Data from MC2P Inventory_Movements:p_wkly_inventory_extract', 
 Sysdate,'wkly_inv_move_extract', 'I');
Commit;


Execute Immediate 'truncate table wkly_inv_move_extract';

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


/* Extract Data From Inventory Movement@MC2P */
-- Insert /*+ Append */ Into Wkly_Inv_Move_Extract
-- Select /*+ FULL(I) PARALLEL(I,8) */ 
--       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,
--       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,
--       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
--       i.Merchandising_Year, i.Merchandising_Week, I.section_id
-- From   (Inventory_Movements@Mc2p) I



If Parm_Start_Year = Parm_End_Year
Then

V_Where := ' Where 

    I.Merchandising_Year = ' ||Parm_Start_Year|| ' and
    I.Merchandising_Week Between '||Parm_Start_Week||' And '|| Parm_End_Week;

execute immediate 'Insert /*+ Append */ Into Wkly_Inv_Move_Extract
      Select /*+ FULL(I) PARALLEL(I,8) */ 
       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,
       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,
       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
       i.Merchandising_Year, i.Merchandising_Week, I.section_id
 From   (Inventory_Movements@Mc2r) I ' || v_where;
 Commit;

Else 

--V_Where := ' Where 

--  (I.Merchandising_Year = ' ||Parm_Start_Year ||' And
--   I.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
--                            Where W.Merch_Year = ' || Parm_Start_Year ||')) Or
--  (I.Merchandising_Year = '||Parm_End_Year||' And
--   I.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W
--                            Where W.Merch_Year = '||Parm_End_Year||'))'; 

For Rec In (Select Distinct Merch_Year From Wkly_Sas_Prod_Time_Pd)
Loop

   For Rec2 In (Select min(Merch_Week) min_week, max(Merch_Week) max_week, Merch_Year From Wkly_Sas_Prod_Time_Pd where merch_year = rec.merch_year group by merch_year)
   Loop

      Insert /*+ Append */ Into Wkly_Inv_Move_Extract
      Select /*+ FULL(I) PARALLEL(I,8) */ 
       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,
       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,
       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
       I.Merchandising_Year, I.Merchandising_Week, I.Section_Id
      From   (Inventory_Movements@Mc2p) I
      Where 

        I.Merchandising_Year = Rec2.Merch_Year And
        I.Merchandising_Week Between Rec2.Min_Week And Rec2.Max_Week;
        Commit;

   end loop;


End Loop;




End If;


--execute immediate 'Insert /*+ Append */ Into Wkly_Inv_Move_Extract
--      Select /*+ FULL(I) PARALLEL(I,8) */ 
--       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,
--       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,
--       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
--       i.Merchandising_Year, i.Merchandising_Week, I.section_id
-- From   (Inventory_Movements@Mc2r) I ' || v_where;



Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 20 And Process_Id = P_Process_Id; 
commit;


/*Submit Next Step*/

Dbms_Job.Submit(Jobno, 'P_Wkly_prod_master();', Sysdate, Null);
Dbms_Job.Submit(Jobno, 'P_Wkly_in_out_trfs();', Sysdate, Null);
Commit;


END P_WKLY_INVENTORY_EXTRACT;


----------------------------------------------------------------------------------


create or replace procedure       P_WKLY_PROD_MASTER 
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


--------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_IM_GROUP 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;



Begin

/*Step 5*/

execute immediate 'truncate table wkly_im_sas_grp';

Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log 
(Process_Id,Process_Step,Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,40,'Step 4:Group Inventory Extract (Site,Sku,Yr,Wk):p_wkly_im_group',Sysdate,'wkly_im_sas_grp','I');
commit;


Update wkly_Prod_Site_List Set Processed_Ind = Null;
Commit;


For Acmd In (Select Site_Prefix From Wkly_Prod_Site_List 
             WHERE processed_ind IS NULL ORDER BY 1)
Loop

    Insert /*+ APPEND */ Into Wkly_Im_Sas_Grp
 Select 

  spl.sku_key,

  Base.Site_Id, 

    Sum(Base.Inven_Move_Qty) Inven_Move_Qty,
  Sum(Base.Retail_Price) Retail_Price,
  Sum(Base.Cost) cost,

  Base.Merchandising_Year,

  Base.merchandising_week

  From

    (Select

    i.site_id, i.style_id,

    SUM(i.inven_move_qty) inven_move_qty,
    sum(i.inven_move_qty *  nvl(i.retail_price,0)) Retail_Price,
  Sum(I.Inven_Move_Qty * (Case I.Inven_Move_Type 
                          When 'TRANSFERS' Then
                             Nvl(I.Landed_Unit_Cost,0)
                          When 'SALES' Then
                             Nvl(Sc.Average_Cost,I.Landed_unit_cost)
                          Else
                             Nvl(I.Landed_Unit_Cost,0)
                          End)
    ) Cost,     

    merchandising_year,

    Merchandising_Week

 From 

   wkly_inv_move_extract I

   Join Styles Sty On (I.Style_Id = Sty.Style_Id)
   Join V_Dept_Class_Subclass V On (v.business_unit_id = '30' and Sty.Section_Id = V.Section_Id)
 --left outer Join Sas_Cost_Lookup scl on (V.Department_Id = Scl.Department_Id) 
    Left Outer Join Site_Style_Cost Sc On (I.Style_Id = Sc.Style_Id And 
                                           I.Site_Id = Sc.Site_Id)
 WHERE

    Substr(I.Site_Id,1,2) = Acmd.Site_Prefix
  AND i.merchandising_year IS NOT NULL
    AND i.merchandising_week IS NOT NULL
    GROUP BY

    I.Site_Id,I.Style_Id, Merchandising_Year, Merchandising_Week
 ) Base

Join (Select Style_Id, Min(Sku_Key) Sku_Key 
      From Sas_Product_Master 
      group by style_id) Spl On (base.Style_Id = Spl.Style_Id )
Group By 

 Base.Site_Id,Spl.Sku_Key, Base.Merchandising_Year, Base.Merchandising_Week ;   


Update Wkly_Prod_Site_List 
 Set Processed_Ind = '1', 

     Processed_Date = Sysdate 
 Where Site_Prefix = Acmd.Site_Prefix;  
commit;


End Loop;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 40 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_ins_calc();', Sysdate, Null);
Commit;


END P_WKLY_IM_GROUP;        




-------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_INS_CALC 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

Parm_Start_Week Number;

Parm_Start_Year Number;

Begin


/*

Process is part of the original way we use to calculate BOP/EOP.

This way will only work for COST and is still used just for cost.

Since it has been learned that to get the correct EOP retail you need to 
follow a special RIM formula.

*/


Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id 

where Process_Type = 'WEEKLY'; 

Select Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) 
Where Rownum = 1;



Insert Into Sas_Process_Log 
   (Process_Id,Process_Step,Process_Name,
    Process_Start_Time,Process_Table,Process_Ind)
Values 

   (P_Process_Id,60,'Step 6:Prepare Cartesian table for Cost:p_wkly_ins_calc',
    Sysdate,'Review Procedure - Multiple Tables','I');
Commit;



Execute Immediate 'drop table wkly_tmp_sas_inventory_calc';
Execute Immediate 'CREATE TABLE wkly_tmp_sas_inventory_calc
PARALLEL (degree 12)

TABLESPACE ERICDATA

PARTITION BY RANGE (merchandising_week)
   INTERVAL ( 1 )

   (PARTITION p1 VALUES LESS THAN (2))
   NOLOGGING

AS

   SELECT site_id,

          sku_key,

          inven_move_qty,

          retail_price,

          cost,

          merchandising_year,
          merchandising_week
     FROM wkly_im_sas_grp

';


Execute Immediate 'create index wtsic_idx0 on wkly_tmp_sas_inventory_calc(site_id, sku_key, merchandising_year, merchandising_week)';

/*

Get one week prior so that aggregated total are added correctly
*/



If Parm_Start_Week = 1

Then

 /* getting week from previous year */
 Insert Into Wkly_Tmp_Sas_Inventory_Calc 
 Select Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
 From

   Sas_Prod_Complete_2012 

 Where Merchandising_Week = (Select Max(Merchandising_Week) From Merchandising_Calendars 
                             Where Merchandising_Year = (Parm_Start_Year - 1) )And
       Merchandising_Year = (Parm_Start_Year - 1);                
else

 Insert Into Wkly_Tmp_Sas_Inventory_Calc 
 Select Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
 From

   Sas_Prod_Complete_2012 

 Where Merchandising_Week = Parm_Start_Week -1 And
       Merchandising_Year = Parm_Start_Year;                
end if;

commit;


execute Immediate 'Truncate Table Wkly_Sas_Sites';
insert into wkly_sas_sites

    SELECT site_id, mod(rownum,30) site_group , 
           '0' processed_ind, 
           to_date('19000101','YYYYMMDD') processed_date, 
           to_date('19000101','YYYYMMDD') start_date
    FROM (

            Select Distinct Site_Id 
            FROM wkly_tmp_sas_inventory_calc
            ORDER BY 1

         );

commit;


update wkly_sas_sites 

   set processed_ind = null, 
       processed_date = null, 
       Start_Date = Null;

commit;



Execute Immediate 'Truncate Table Wkly_Sas_Styles';

Insert Into Wkly_Sas_Styles
Select Distinct Style_Id 

From Wkly_Tmp_Sas_Inventory_Calc I 
join sas_product_master s on (s.sku_key = I.Sku_Key);
commit;




Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 60 And Process_Id = P_Process_Id; 
Commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_cart_calc();', Sysdate, Null);
Commit;


END P_WKLY_INS_CALC;





----------------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_CART_CALC 
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






-----------------------------------------------------------------------


create or replace procedure        P_WKLY_SAS_INV 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

Parm_Start_Week Number;

Parm_Start_Year Number;


Begin

/*

Process is part of the original way we use to calculate BOP/EOP.

Final Step in calculating EOP Cost

Since it has been learned that to get the correct EOP retail you need to 
follow a special RIM formula.

*/

Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Select Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year
From (Select * From Wkly_Sas_Prod_Time_Pd 
      Order By Merch_Year Asc, Merch_Week Asc) 
Where Rownum = 1;


Insert Into Sas_Process_Log 
(Process_Id,Process_Step,Process_Name,
 Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,180,'Step 18:Aggregate Cost Totals by weeks:p_wkly_sas_inv',Sysdate,'wkly_sas_inv','I');
commit;


  Execute Immediate 'truncate table wkly_sas_inv'; 
  Update Wkly_Sas_Sites Set Processed_Ind = Null, Processed_Date = Null, Start_Date = Null; 
  Commit; 



  For Asitegroup In (Select Distinct Site_Group From Wkly_Sas_Sites 
                     Where Processed_Ind Is Null Order By Site_Group) 
  Loop 

    Update Wkly_Sas_Sites Set Start_Date = Sysdate 
    WHERE site_group = asitegroup.site_group;
    commit;



    Insert /*+ APPEND */ Into Wkly_Sas_Inv 
     (Site_Id,Sku_Key,Merchandising_Year,Merchandising_Week,
      Inventory_On_Week,Retail_On_Week,Cost_On_Week) 
    SELECT 

      Site_Id, 

      Sku_Key, 

      Merchandising_Year, 

      Merchandising_Week, 

      Inventory_On_Week, 

      Retail_On_Week, 

      Cost_On_Week 

    From ( 

      SELECT 

        Site_Id, 

        Sku_Key, 

        Merchandising_Year, 
        Merchandising_Week, 
        SUM(inven_move_qty) OVER ( 
          Partition By Site_Id, Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Inventory_On_Week, 
        Sum(Retail_Price) Over ( 
          Partition By Site_Id, Sku_Key 
          Order By Merchandising_Year , Merchandising_Week ) Retail_On_Week, 
        Sum(Cost) Over ( Partition By Site_Id, Sku_Key 
        Order By Merchandising_Year , Merchandising_Week ) Cost_On_Week 
      From Wkly_Tmp_Sas_Inventory_Calc C 
      Where 

        Site_Id In (Select Site_Id From Wkly_Sas_Sites 
                    Where Site_Group = Asitegroup.Site_Group) 
        ) Base 

    Where Inventory_On_Week <> 0 And 
          Exists (Select 1 From Wkly_Sas_Prod_Time_Pd 
                  Where Merch_Year = Base.Merchandising_Year And 
                        merch_week = base.merchandising_week);


    Update Wkly_Sas_Sites Set Processed_Ind = '1', Processed_Date = Sysdate 
    Where Site_Group = Asitegroup.Site_Group;   
    Commit; 

  End Loop; 


If Parm_Start_Week = 1

Then

 /* getting week from previous year */

Insert Into Wkly_Sas_Inv(Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, Merchandising_Year, Merchandising_Week)
Select Site_Id, Sku_Key, 0 Inventory_On_Week, 
      0 Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
    From

      Sas_Prod_Complete_2012 
 Where Merchandising_Week = (Select Max(Merchandising_Week) From Merchandising_Calendars 
                             Where Merchandising_Year = (Parm_Start_Year - 1) )And
       Merchandising_Year = (Parm_Start_Year - 1);                
else


Insert Into Wkly_Sas_Inv(Site_Id, Sku_Key, Inventory_On_Week, 
       Retail_On_Week , Cost_On_Week, Merchandising_Year, Merchandising_Week)
Select Site_Id, Sku_Key, 0 Inventory_On_Week, 
      0 Retail_On_Week , Cost_On_Week, merchandising_year, merchandising_week
    From

      Sas_Prod_Complete_2012 
 Where Merchandising_Week = Parm_Start_Week -1 And
       Merchandising_Year = Parm_Start_Year;                
end if;

Commit;





Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 180 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_Sum_Cart();', Sysdate, Null);
Commit;



END P_WKLY_SAS_INV;         


---------------------------------------------------------------------------


create or replace procedure        P_WKLY_SUM_CART 
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


-------------------------------------------------------------------------------


create or replace procedure        P_WKLY_SAS_BOP_EOP 
Is 
                        
                           
P_Process_Id Number;

Parm_Start_Week Number;

Parm_Start_year Number;

Parm_End_Week Number;

Parm_Period Number;

Parm_year Number;

Jobno Binary_Integer;


BEGIN

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 
Select Merch_Week Into Parm_End_Week From (Select * From Wkly_Sas_Prod_Time_Pd Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;
Select Merch_Week, merch_year Into Parm_Start_Week, Parm_Start_year From (Select * From Wkly_Sas_Prod_Time_Pd Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id,210,'Step 21:Process EOP using RIM formula:p_wkly_sas_bop_eop',Sysdate,'wkly_sas_bop_eop','I');
commit;


Execute Immediate 'truncate table New_Wkly_Eop'; 

/* SQL manipulation to get the min period for the past 5 weeks even during 
   cross merch year eg 2015 thru 2016 */
select merchandising_period, merchandising_year into parm_period, parm_year from
 (select * from

  (select * from 

   (select merchandising_year, merchandising_week, 
            merchandising_period 
     from  merchandising_calendars
     where week_ending_date <= sysdate
     order by 1 desc, 2 desc)
   where rownum < 6) 

  order by 1, 2)

where rownum = 1;



--if parm_period = 11 

--then 

--rdiusr.sas_reversal_sp@mc2p(parm_year);
--

--Execute Immediate 'drop table Sas_Shrink_Reverse';
--

--Execute Immediate '

--create table Sas_Shrink_Reverse as
--select * from sas_shrink_reverse_sku@mc2p';
--end if;





For rec In (Select * From Wkly_Sas_Prod_Time_Pd)
Loop

Insert Into New_Wkly_Eop

(Site_Id, Sku_Key,  Merchandising_Year,  Merchandising_Week, Retail_On_Week, inventory_on_week)
Select 

      Site_Id, Sku_Key, 

      Merchandising_Year, 

      Merchandising_Week, 

      Sum(

       (nvl(retail_on_week,0)  
      + Nvl(Purchases,0) 

      + Nvl(Transfers_Retail,0) 
      + Nvl(Mark_Up_Perm,0)
      - Nvl(Sales_Sold_Price,0)
      - Nvl(Mark_Down_Perm,0) 
      - Nvl(Mark_Down_Pos,0)       
      - Nvl(Sales_Shrink,0)
      - nvl(Adjustments_retail,0))) Retail_On_Week,
      sum(inventory_on_week)
    From

    ((Select 

      spc.Site_Id, Spg1.Sku_Key, Spm1.Style_Id, Rec.Merch_Year Merchandising_Year, 
      rec.merch_week Merchandising_Week,  Inventory_On_Week, 
      Retail_On_Week, 0 Cost_On_Week, 0 Mark_Down_Perm, 0 Mark_Up_Perm, 0 Mark_Down_Pos,
      Case When Parm_Start_Week = 49
      Then 

       (Ssr.Shrink_Reverse * -1)
      Else

       0 

      end Sales_Shrink, 0 Sales_Sold_Price, 0 Purchases,  0 Transfers_Retail, 0 Adjustments_retail
    From

      Sas_Prod_Complete_2012 Spc      
      Join Sas_Product_Master Spm1 On (Spm1.Sku_Key = Spc.Sku_Key)
      Join (Select Style_Id, Min(Sku_Key) Sku_Key 
            From Sas_Product_Master
            Group By Style_Id) Spg1 On (Spm1.Style_Id = Spg1.Style_Id)
      Left Join Sas_Shrink_Reverse Ssr On (Ssr.Site_Id = Spc.Site_Id And
                                           Ssr.Style_Id = Spm1.Style_Id And
                                           --Spc.Prev_Retail_On_Week <> 0 And
                                           Ssr.Merchandising_Year = Parm_Start_Year And
                                           Ssr.Merchandising_Week = Parm_Start_Week)

        Where

          Spc.Merchandising_Year = (Case Parm_Start_Week When 1 
                                    Then (Parm_Start_year -1)
                                     Else Parm_Start_year End) And
          Spc.Merchandising_Week = (Case Parm_Start_Week When 1 
                                    Then (Select Max(Merchandising_Week) From Merchandising_Calendars
                                       where Merchandising_Year = Parm_Start_year -1)
                                    Else (Parm_Start_Week - 1) End))


    Union all

    (Select Site_Id, spg2.Sku_Key, spm2.style_id, Merchandising_Year, Merchandising_Week, inven_qty Inventory_On_Week, 
      0 Retail_On_Week, 0 Cost_On_Week, Mark_Down_Perm, Mark_Up_Perm, Mark_Down_Pos,
      Case When 12 = (Select Distinct M.Merchandising_Period From Merchandising_Calendars M
                      Where M.Merchandising_Week = Rec.Merch_Week And
                            M.Merchandising_Year = Rec.Merch_Year)
      Then 0

      Else Sales_Shrink

      End Sales_Shrink , Sales_Sold_Price, Purchases,  Transfers_Retail, Adjustments_Retail 
     From

      Wkly_Sum_Totals Wst

      Join Sas_Product_Master Spm2 On (Spm2.Sku_Key = Wst.Sku_Key)
      Join (Select Style_Id, Min(Sku_Key) Sku_Key From Sas_Product_Master
            group by style_id) spg2 on (spm2.style_id = spg2.style_id)
      Where

          Merchandising_Year = Rec.Merch_Year And
          Merchandising_Week = rec.merch_week)) Base


   Group By        

   Site_Id, Sku_Key,  Merchandising_Year, Merchandising_Week having (sum(inventory_on_week) <> 0 or Sum(
       (nvl(retail_on_week,0)  
      + Nvl(Purchases,0) 

      + Nvl(Transfers_Retail,0) 
      + Nvl(Mark_Up_Perm,0)
      - Nvl(Sales_Sold_Price,0)
      - Nvl(Mark_Down_Perm,0) 
      - Nvl(Mark_Down_Pos,0)       
      - Nvl(Sales_Shrink,0)
      - nvl(Adjustments_retail,0))) <> 0);
Commit;

End Loop;



Insert Into New_Wkly_Eop

 (Site_Id, Sku_Key,  Merchandising_Year,  Merchandising_Week, Retail_On_Week, Inventory_On_Week)
Select Site_Id, Sku_Key,  Merchandising_Year,  Merchandising_Week, Retail_On_Week, Inventory_On_Week
From 

 New_Wkly_Eop_2012

Where

 Merchandising_Year = (Case Parm_Start_Week When 1 
                         Then (Parm_Start_Year -1)
                         Else Parm_Start_Year 
                       End) And
 Merchandising_Week = (Case Parm_Start_Week When 1 
                         Then (Select Max(Merchandising_Week) From Merchandising_Calendars
                                Where Merchandising_Year = Parm_Start_Year -1)
                          Else (Parm_Start_Week - 1) 
                       End);


      Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 210 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_merge_eop_cost();', Sysdate, Null);
commit;


END P_WKLY_SAS_bop_eop;     

------------------------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_MERGE_EOP_COST 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 220,'Step 22:Merge/Insert Process EOP Cost:p_wkly_merge_eop',Sysdate,'sas_prod_complete','I');
Commit;

--wkly_sas_inv

MERGE  /*+ APPEND */

Into Sas_Prod_Complete Tgt

Using wkly_sas_inv Src

 On ( Src.Site_Id = Tgt.Site_Id And
      Src.Sku_Key = Tgt.Sku_Key And
      Src.Merchandising_Year = Tgt.Merchandising_Year And
      src.merchandising_week = tgt.merchandising_week)
WHEN MATCHED

THEN

UPDATE

Set 


Tgt.Cost_On_Week = Src.Cost_On_Week
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
Tgt.Prev_Inv_On_Week, Tgt.Prev_Retail_On_Week, Tgt.Prev_Cost_On_Week, Tgt.Sales_Sold_Price, Tgt.Inv_Qty_Wkly,
Tgt.Adjustments_Retail, Tgt.Adjustments_Cost, Tgt.Adjustments_Qty
)

Values ( 

Src.Site_Id, Src.Sku_Key, Src.Merchandising_Year, Src.Merchandising_Week,
0, 0, Src.Cost_On_Week, 

       0,0,0,  

       0,0,0,  

       0,0,0,  

       0,0,0,   

       0,0,0,0,

       0,0,0,0,0,

       0,0,0);

Commit;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 220 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_WKLY_MERGE_BOP_COST();', Sysdate, Null);
commit;


END P_WKLY_MERGE_EOP_COST;  


--------------------------------------------------


create or replace procedure        P_WKLY_MERGE_BOP_COST 
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

-----------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_MERGE_EOP_RETAIL 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 240,'Step 24:Merge/Insert Process EOP Retail/Quantity:p_wkly_merge_eop_retail',Sysdate,'sas_prod_complete','I');
Commit;

Merge /*+ APPEND */ Into Sas_Prod_Complete Tgt 
Using new_wkly_eop Src On 

( Src.Site_Id = Tgt.Site_Id And 
Src.Sku_Key = Tgt.Sku_Key And 
Src.Merchandising_Year = Tgt.Merchandising_Year And 
Src.Merchandising_Week = Tgt.Merchandising_Week) 
When Matched Then 

Update 

Set Tgt.Retail_On_Week = Src.Retail_On_Week,
Tgt.inventory_On_Week = Src.inventory_On_Week 
When Not Matched Then 

Insert ( Tgt.Site_Id, Tgt.Sku_Key, Tgt.Merchandising_Year, Tgt.Merchandising_Week, 
Tgt.Inventory_On_Week, Tgt.Retail_On_Week, Tgt.Cost_On_Week, 
Tgt.Sales_Retail, Tgt.Sales_Cost, Tgt.Sales_Qty, 
Tgt.Receipts_Retail, Tgt.Receipts_Cost, Tgt.Receipts_Qty, 
Tgt.Returns_Retail, Tgt.Returns_Cost, Tgt.Returns_Qty, 
Tgt.Transfers_Retail, Tgt.Transfers_Cost, Tgt.Transfers_Qty, 
Tgt.Mark_Down_Perm, Tgt.Mark_Up_Perm, Tgt.Mark_Down_Pos, Tgt.Mark_Up_Pos,
Tgt.Prev_Inv_On_Week, Tgt.Prev_Retail_On_Week, Tgt.Prev_Cost_On_Week, Tgt.Sales_Sold_Price,Tgt.Inv_Qty_Wkly,
Tgt.Adjustments_Retail, Tgt.Adjustments_Cost, Tgt.Adjustments_Qty )
Values ( Src.Site_Id, Src.Sku_Key, Src.Merchandising_Year, Src.Merchandising_Week, 
src.inventory_on_week, Src.Retail_On_Week, 0,
0,0,0, 

0,0,0, 

0,0,0, 

0,0,0, 

0,0,0,0, 

0,0,0,0,0,

0,0,0); 

Commit; 



Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 240 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_WKLY_MERGE_BOP_RETAIL();', Sysdate, Null);
Commit;


END P_WKLY_MERGE_EOP_RETAIL;




----------------------------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_MERGE_BOP_RETAIL 
Is 
                        
                           
P_Process_Id Number;

Jobno Binary_Integer;

Max_Week Number;


Begin

/*Step 8.2 */

Select Max(Process_Id) Into P_Process_Id From Sas_Process_Log_Id where Process_Type = 'WEEKLY'; 

Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,Process_Start_Time,Process_Table,Process_Ind)
Values (P_Process_Id, 250,'Step 25:Merge/Insert Process BOP Retail/Quantity:p_wkly_merge_bop_retail',Sysdate,'sas_prod_complete','I');
Commit;


For Rec In (Select w.Merchandising_Week, w.Merchandising_Year From new_wkly_eop w
            Group By Merchandising_Year, Merchandising_Week Order By Merchandising_Year, Merchandising_Week)
Loop

Select Max(M.Merchandising_Week) Into Max_Week From Merchandising_Calendars M 
where m.merchandising_year = rec.merchandising_year;

MERGE /*+ APPEND */

Into Sas_Prod_Complete Tgt

Using (Select * From New_Wkly_Eop Nw 
       Where nw.Merchandising_Week = Rec.Merchandising_Week And 
             nw.Merchandising_Year = rec.merchandising_year ) Src
On ( Src.Site_Id = Tgt.Site_Id And
Src.Sku_Key = Tgt.Sku_Key And
    (Case Src.Merchandising_Week When Max_Week
       Then (Src.Merchandising_Year + 1)
       Else Src.Merchandising_Year End) = Tgt.Merchandising_Year And
   (Case Src.Merchandising_Week When Max_Week
      Then 1

      Else (Src.Merchandising_Week + 1) end) = Tgt.Merchandising_Week )
WHEN MATCHED

THEN

UPDATE

Set 

Tgt.Prev_Retail_On_Week = Src.Retail_On_Week,
Tgt.Prev_inv_On_Week = Src.inventory_On_Week

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

 Src.Inventory_On_Week, Src.Retail_On_Week, 0,0,0,
 0,0,0);

Commit;

End Loop;


Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 250 And Process_Id = P_Process_Id; 
commit;



Dbms_Job.Submit(Jobno, 'P_Wkly_merge_end();', Sysdate, Null);
Commit;



END P_WKLY_MERGE_BOP_RETAIL;



--------------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_MERGE_END 
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


----------------------------------------------------------------------------------------------------------------------------


create or replace PROCEDURE p_wkly_time_period_sp IS 
/*Step 1 */
    p_process_id      NUMBER;
    jobno             BINARY_INTEGER;
    parm_start_week   VARCHAR2(25);
    parm_end_week     VARCHAR2(25);
BEGIN

/* Clear Table to Insert New Weeks*/
    EXECUTE IMMEDIATE 'Truncate Table wkly_sas_prod_time_pd';
    UPDATE sas_process_sw
        SET
            process_time = SYSDATE,
            process_complete = 'false';

    COMMIT;
    
    INSERT INTO sas_process_log_id (
        process_ind,
        process_date,
        process_type
    ) VALUES (
        'I',
        SYSDATE,
        'WEEKLY'
    );

    COMMIT;

/* Get Process_id from Log */
    SELECT
        MAX(process_id)
    INTO
        p_process_id
    FROM
        sas_process_log_id
    WHERE
        process_type = 'WEEKLY'; 
/*Process/Insert Week*/

--    INSERT INTO wkly_sas_prod_time_pd SELECT
--        merchandising_year,
--        merchandising_week
--    FROM
--        (
--            SELECT
--                *
--            FROM
--                sas_process_calendar
--            WHERE
--                processed = 'N'
--            ORDER BY
--                1,
--                2
--        )
--    WHERE
--        ROWNUM < 5;


    IF
        SQL%rowcount = 0
    THEN
        return;
    END IF;

  Insert Into Wkly_Sas_Prod_Time_Pd values ('2022','1');
  Insert Into Wkly_Sas_Prod_Time_Pd values ('2022','2');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','13');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','14');
--  Insert Into Wkly_Sas_Prod_Time_Pd values ('2016','15');

    COMMIT;

/*Write to Log*/
    INSERT INTO sas_process_log (
        process_id,
        process_step,
        process_name,
        process_start_time,
        process_table,
        process_ind
    ) VALUES (
        p_process_id,
        10,
        'Step 1:Get weeks to process:p_wkly_time_period SPECIAL',
        SYSDATE,
        'Wkly_Sas_Prod_Time_Pd',
        'I'
    );

    COMMIT;

/*Get Ending week */
    SELECT
        merch_year
         || '.'
         || lpad(
            merch_week,
            2,
            '0'
        )
    INTO
        parm_end_week
    FROM
        (
            SELECT
                *
            FROM
                wkly_sas_prod_time_pd
            ORDER BY
                merch_year DESC,
                merch_week DESC
        )
    WHERE
        ROWNUM = 1;

/*Get Starting Week*/

    SELECT
        merch_year
         || '.'
         || lpad(
            merch_week,
            2,
            '0'
        )
    INTO
        parm_start_week
    FROM
        (
            SELECT
                *
            FROM
                wkly_sas_prod_time_pd
            ORDER BY
                merch_year ASC,
                merch_week ASC
        )
    WHERE
        ROWNUM = 1;

    UPDATE sas_process_log
        SET
            process_ind = 'C',
            process_end_time = SYSDATE,
            process_name = 'Step 1:Get weeks to process:p_wkly_time_period: ' || parm_start_week || ' thru ' || parm_end_week
    WHERE
            process_step = 10
        AND
            process_id = p_process_id;

    COMMIT;
    
    UPDATE sas_process_log
        SET
            process_ind = 'C',
            process_end_time = SYSDATE
    WHERE
            process_step = 10
        AND
            process_id = p_process_id;

    COMMIT;
    
    dbms_job.submit(
        jobno,
        'P_Wkly_Inventory_Extract();',
        SYSDATE,
        NULL
    );
    COMMIT;
END p_wkly_time_period_sp;


-----------------------------------------------------------------------------------------------------------------------------------------


create or replace procedure        P_WKLY_IN_OUT_TRFS 
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



------------------------------------------------------------------