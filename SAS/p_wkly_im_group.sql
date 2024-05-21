SQL> l
  1* select text from user_source where name like upper('P_WKLY_IM_GROUP')
SQL> /

procedure        P_WKLY_IM_GROUP                                                
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
Values (P_Process_Id,40,'Step 4:Group Inventory Extract (Site,Sku,Yr,Wk):p_wkly_
im_group',Sysdate,'wkly_im_sas_grp','I');                                       
                                                                                
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

   Join V_Dept_Class_Subclass V On (v.business_unit_id = '30' and Sty.Section_Id
 = V.Section_Id)                                                                
                                                                                
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

119 rows selected.

SQL> spool off
