--------------------------------------------------------
--  DDL for Procedure P_WKLY_MARK_DOWN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."P_WKLY_MARK_DOWN" 
Is 
                        
                           
P_Process_Id Number;


Parm_Start_Week Number;

Parm_Start_Year Number;


Parm_End_Week Number;

Parm_End_Year Number;


Jobno Binary_Integer;


V_Where  Varchar2(500);



Begin

/*Step  */

--Execute Immediate 'Truncate Table sas_prchg_lookup';
--Execute Immediate 'Truncate Table sas_prchg_lookup_sales';

Select Max(Process_Id) Into P_Process_Id 
From Sas_Process_Log_Id Where Process_Type = 'WEEKLY'; 

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


Insert Into Sas_Process_Log (Process_Id,Process_Step,Process_Name,
                             Process_Start_Time,Process_Table,Process_Ind)
Values 

(P_Process_Id,90,'Step 9:Process Data needed for Markdown/Markups:p_wkly_mark_down',
 Sysdate,'Multiple Tables','I');
Commit;



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


execute immediate 'Delete Mark_Down_Collection_All base ' || v_where;
Commit;


execute immediate '

Insert Into Mark_Down_Collection_All Mdc
(Mdc.Site_Id, Mdc.Style_Id, Mdc.Merchandising_Year, Mdc.Merchandising_Week,
Mdc.Mark_Down_Perm, Mdc.Mark_Down_Pos, Mdc.Mark_Up_Perm )
Select base.Site_Id,base.Style_Id,base.Merchandising_Year, base.Merchandising_Week,
Sum(

  Case When base.Reason_Sub_Type in (''HOPCH'',''TRX'')
  Then 

    Case When base.Operation = ''MARKDOWN''
      Then base.MarkDown_value
      Else 0

    End

  Else 0

  End) Mark_Down_Perm, 

  Sum(Case When base.Reason_Sub_Type = ''POSPCH''
  Then 

    Case When base.Operation = ''MARKDOWN''
      Then base.MarkDown_value
      Else 0

    End

  Else 0

  End ) Mark_Down_Pos, 

  Sum(Case When base.Reason_Sub_Type  in (''HOPCH'',''TRX'')
  Then 

    Case When base.Operation = ''MARKUP''
      Then base.MarkDown_value
      Else 0

    End

  Else 0

  End) Mark_Up_Perm From (Prchg_Style_Weekly_Stats@Mc2p) base ' 
  || v_where ||   

  ' And base.Reason_Sub_Type In (''HOPCH'', ''POSPCH'', ''TRX'')
Group By base.Site_Id,base.Style_Id,base.Merchandising_Year,base.Merchandising_Week';
commit;




Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate 
Where Process_Step = 90 And Process_Id = P_Process_Id; 
commit;


Dbms_Job.Submit(Jobno, 'P_Wkly_merge_prep();', Sysdate, Null);
Commit;


END P_WKLY_MARK_down;       

/
