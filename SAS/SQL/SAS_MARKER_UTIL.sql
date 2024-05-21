--------------------------------------------------------
--  DDL for Procedure SAS_MARKER_UTIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."SAS_MARKER_UTIL" 
(p_op In char,p_marker_file In varchar2)
is
fHandle  UTL_FILE.FILE_TYPE;
  
Begin

if p_op = 'C' and does_sas_marker_exist(p_marker_file) = 'N' then
   fHandle := UTL_FILE.FOPEN('FLASH', p_marker_file, 'w');
   UTL_FILE.PUT(fHandle, '');
   UTL_FILE.FCLOSE(fHandle);
end if;

if p_op = 'R' and does_sas_marker_exist(p_marker_file) = 'Y' then
   Utl_File.Fremove('FLASH',p_marker_file);
end if;

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
    Raise;
    
End;

/
