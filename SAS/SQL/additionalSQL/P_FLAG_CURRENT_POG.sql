--------------------------------------------------------
--  DDL for Procedure P_FLAG_CURRENT_POG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "MAXDATA"."P_FLAG_CURRENT_POG" ( p_period IN INTEGER,  p_cycle IN INTEGER)
as

   cursor  c_pogmaster is
    select pog_master_id, pog_start_cycle, pog_start_period, pog_end_cycle,
           pog_end_period, pog_model_id
    from   maxdata.pogmaster
    where  approval_status = 1 ;

   t_pog_master_id      integer ;
   t_pog_start_cycle    numeric(6) ;
   t_pog_start_period   numeric(6) ;
   t_pog_end_cycle      numeric(6) ;
   t_pog_end_period     numeric(6) ;
   t_pog_model_id       integer ;


   begin

   open c_pogmaster ;
    loop
      fetch c_pogmaster
       into t_pog_master_id, t_pog_start_cycle, t_pog_start_period, t_pog_end_cycle,
            t_pog_end_period, t_pog_model_id ;

       exit when c_pogmaster%notfound;

       if t_pog_start_cycle||lpad(t_pog_start_period,2,0) <= p_cycle||lpad(p_period,2,0) and
           ( t_pog_end_cycle||lpad(t_pog_end_period,2,0) > p_cycle||lpad(p_period,2,0) or
           nvl(t_pog_end_cycle,0)=0 or nvl(t_pog_end_period,0)=0 ) then
        update pogmaster
        set    current_pog = 1, pog_mvmt_applied = 1
        where  pog_master_id = t_pog_master_id ;
      else
        update pogmaster
        set    current_pog = 0
        where  pog_master_id = t_pog_master_id ;
      end if ;
    end loop ;
  end ;

/

  GRANT EXECUTE ON "MAXDATA"."P_FLAG_CURRENT_POG" TO "MADMAX";
  GRANT EXECUTE ON "MAXDATA"."P_FLAG_CURRENT_POG" TO "MAXUSER";
