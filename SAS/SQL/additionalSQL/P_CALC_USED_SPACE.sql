--------------------------------------------------------
--  DDL for Procedure P_CALC_USED_SPACE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_CALC_USED_SPACE" 
  ( rotate IN NUMBER, slope  IN NUMBER, orient IN NUMBER,
    height IN NUMBER, width  IN NUMBER, depth  IN NUMBER,
    x_face IN NUMBER, y_face IN NUMBER, z_face IN NUMBER,
    p_used_cubic OUT NUMBER, p_used_dsp OUT NUMBER,
    p_used_flr OUT NUMBER, p_used_linear OUT NUMBER )
as

  temp_var  number(16,9) ;
  t_height  number(16,9) ;
  t_width   number(16,9) ;
  t_depth   number(16,9) ;

begin

  temp_var := NULL ;
  t_height := height ;
  t_width  := width ;
  t_depth  := depth ;

  -- height becomes width, width becomes height, depth unchanged
  if nvl(rotate,0) between 45 and 135 then
    temp_var := t_height ;
    t_height := t_width ;
    t_width  := temp_var ;
  end if ;

  -- depth becomes height, height becomes depth, width unchanged
  if nvl(slope,0) between 45 and 135 then
    temp_var := t_depth ;
    t_depth  := t_height ;
    t_height := temp_var ;
  end if ;

  -- width becomes depth, depth becomes width, height unchanged
  if nvl(orient,0) between 45 and 135 then
    temp_var := t_width ;
    t_width  := t_depth ;
    t_depth  := temp_var ;
  end if ;

  -- cubic
  p_used_cubic := (t_height*y_face) * (t_width*x_face) * (t_depth*z_face) ;

  -- floor
  p_used_flr := (t_width*x_face) * (t_depth*z_face) ;

  -- display
  p_used_dsp := (t_height*y_face) * (t_width*x_face) ;

  -- linear
  p_used_linear := (t_width*x_face) ;

end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_CALC_USED_SPACE" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_CALC_USED_SPACE" TO "MAXUSER";
