--------------------------------------------------------
--  DDL for Procedure COMPILE_INVALID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."COMPILE_INVALID" AS
v_count NUMBER := 0;
BEGIN
  FOR cur_rec IN (SELECT object_type,owner,object_name
  FROM all_objects
  WHERE owner = 'ERIC'
  AND object_type IN ('PROCEDURE','FUNCTION','TRIGGER','PACKAGE','PACKAGE_BODY')
  AND status <> 'VALID'
  ORDER BY object_name, object_type desc)
  LOOP
    DBMS_DDL.alter_compile(cur_rec.object_type,cur_rec.owner,cur_rec.object_name);
    v_count := v_count + 1;
  END LOOP;
  
  FOR cur_rec IN (SELECT object_name
  FROM all_objects
  WHERE owner = 'ERIC'
  AND object_type IN ('VIEW')
  AND status <> 'VALID'
  ORDER BY object_name, object_type desc)
  LOOP
    execute immediate 'ALTER VIEW ' || cur_rec.object_name || ' COMPILE';
    v_count := v_count + 1;
  END LOOP;

  commit;  
  dbms_output.put_line(v_count || ' invalid objects recompiled');
END;

/
