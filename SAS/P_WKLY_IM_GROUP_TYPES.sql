SQL> L
  1* select text from user_source where name like upper('P_WKLY_IM_GROUP_TYPES')
SQL> /

procedure        P_WKLY_IM_GROUP_TYPES                                          
Is                                                                              
                                                                                
                                                                                
P_Process_Id Number;                                                            
                                                                                
Jobno Binary_Integer;                                                           
                                                                                
                                                                                
Begin                                                                           
                                                                                
/*Step 5*/                                                                      
                                                                                

Execute Immediate 'truncate table wkly_im_sas_grp_types';                       
                                                                                
Select Max(Process_Id) Into P_Process_Id                                        
From Sas_Process_Log_Id where Process_Type = 'WEEKLY';                          
                                                                                
Insert Into Sas_Process_Log                                                     
 (Process_Id,Process_Step,Process_Name,                                         
  Process_Start_Time,Process_Table,Process_Ind)                                 
Values                                                                          
                                                                                
(P_Process_Id,50,'Step 5:Group Inventory Extract (Site,Sku,Type,Yr,Wk):p_wkly_im
_group_types',Sysdate,'wkly_im_sas_grp_types','I');                             
                                                                                

Commit;                                                                         
                                                                                
                                                                                
Update Wkly_Prod_Site_List_Types Set Processed_Ind = Null;                      
                                                                                
For Acmd In (Select Site_Prefix From Wkly_Prod_Site_List_Types                  
             WHERE processed_ind IS NULL ORDER BY 1)                            
Loop                                                                            
                                                                                
    INSERT /*+ APPEND */ INTO wkly_Im_Sas_Grp_Types                             
  Select                                                                        
                                                                                
  spl.sku_key,                                                                  

                                                                                
  Base.Site_Id,                                                                 
                                                                                
  base.inven_move_type,                                                         
                                                                                
    Sum(Base.Inven_Move_Qty) Inven_Move_Qty,                                    
  Sum(Base.Retail_Price) Retail_Price,                                          
  Sum(Base.Retail_Price_V2) Retail_Price_V2,                                    
  Sum(Base.Cost) cost,                                                          
                                                                                
  Base.Merchandising_Year,                                                      
                                                                                
  Base.merchandising_week                                                       

                                                                                
  From                                                                          
                                                                                
    (Select                                                                     
                                                                                
    I.Site_Id, I.Style_Id,                                                      
                                                                                
  Decode(I.Inven_Move_Type, 'RECEIVING', 'RECEIVING',                           
                              'TRANSFERS', 'TRANSFERS',                         
                              'RETURNS', 'RETURNS',                             
                              'SALES', 'SALES',                                 
                              'ADJUSTMENT','ADJUSTMENT',                        
                              'PHYSICAL', 'ADJUSTMENT') inven_move_type,        

    SUM(i.inven_move_qty) inven_move_qty,                                       
    sum(i.inven_move_qty * (CASE when i.merchandising_year < 2011 then          
                                    nvl(i.retail_price_final,0)                 
                                else                                            
                                    CASE when i.inven_move_type = 'SALES' then  
                                              nvl(i.retail_price_final,0)       
                                         else                                   
                                              nvl(i.retail_price,0)             
                                    End                                         
                            END)                                                
        ) Retail_Price,                                                         
                                                                                
    SUM(i.inven_move_qty *  nvl(i.retail_price,0)* (case when I.Inven_Move_Type 

= 'PHYSICAL' Then -1 else 1 end)) Retail_Price_v2,                              
                                                                                
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

                                                                                
    merchandising_week                                                          
                                                                                
    From wkly_inv_move_extract I                                                
    Join Styles Sty On (I.Style_Id = Sty.Style_Id)                              
    Join V_Dept_Class_Subclass V On (v.business_unit_id = '30' and Sty.Section_I
d = V.Section_Id)                                                               
                                                                                
    left outer Join Sas_Cost_Lookup scl on (V.Department_Id = Scl.Department_Id)
                                                                                
                                                                                
    Left Outer Join Site_Style_Cost Sc On (I.Style_Id = Sc.Style_Id And         
                                           I.Site_Id = Sc.Site_Id)              

    Where I.Inven_Move_Type In ('RECEIVING','TRANSFERS','RETURNS','SALES','ADJUS
TMENT','PHYSICAL')                                                              
                                                                                
                                                                                
                                                                                
    And Substr(I.Site_Id,1,2) = Acmd.Site_Prefix                                
      And I.Merchandising_Year Is Not Null                                      
      AND i.merchandising_week IS NOT NULL                                      
    Group By                                                                    
                                                                                
      I.Site_Id,I.Style_Id,                                                     
                                                                                
    Decode(I.Inven_Move_Type, 'RECEIVING', 'RECEIVING',                         

                              'TRANSFERS', 'TRANSFERS',                         
                              'RETURNS', 'RETURNS',                             
                              'SALES', 'SALES',                                 
                              'ADJUSTMENT','ADJUSTMENT',                        
                              'PHYSICAL', 'ADJUSTMENT'),                        
      Merchandising_Year, Merchandising_Week                                    
    ) Base                                                                      
                                                                                
Join (Select Style_Id, Min(Sku_Key) Sku_Key                                     
      From Sas_Product_Master                                                   
      group by style_id) Spl On (base.Style_Id = Spl.Style_Id )                 
                                                                                
                                                                                

 Group By Base.Site_Id,spl.sku_key, Base.Inven_Move_Type,                       
    Base.Merchandising_Year, Base.Merchandising_Week                            
    ;                                                                           
                                                                                
/*                                                                              
                                                                                
    Where (I.Inven_Move_Type In ('RECEIVING','TRANSFERS','RETURNS','SALES','ADJU
STMENT')                                                                        
                                                                                
    Or                                                                          
                                                                                
         (Inven_Move_Type ='PHYSICAL' And                                       
          12 = (Select Distinct M.Merchandising_Period From Merchandising_Calend

ars M                                                                           
                                                                                
                           Where M.Merchandising_Year = I.Merchandising_Year And
                                                                                
                                                                                
                                 M.Merchandising_Week = I.Merchandising_Week)   
         )                                                                      
                                                                                
        )                                                                       
                                                                                
*/                                                                              
                                                                                
                                                                                

                                                                                
                                                                                
Update Wkly_Prod_Site_List_Types                                                
 Set Processed_Ind = '1',                                                       
                                                                                
 Processed_Date = Sysdate                                                       
                                                                                
WHERE site_prefix = acmd.site_prefix;                                           
commit;                                                                         
                                                                                
End Loop;                                                                       
                                                                                
                                                                                

/*                                                                              
                                                                                
 (Decode(Scl.Cost_Key, 'A', Nvl(I.Average_Cost,0),                              
   'L', Nvl(I.Landed_Unit_Cost,0),                                              
   'M', Nvl(Sc.Average_Cost,0),                                                 
   nvl(nvl(I.average_cost,I.landed_unit_cost),0) ))                             
*/                                                                              
                                                                                
Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate        
Where Process_Step = 50 And Process_Id = P_Process_Id;                          
commit;                                                                         
                                                                                
                                                                                

Dbms_Job.Submit(Jobno, 'P_Wkly_types();', Sysdate, Null);                       
Commit;                                                                         
                                                                                
                                                                                
                                                                                
END P_WKLY_IM_GROUP_TYPES;                                                      

172 rows selected.

SQL> SPOOL OFF
