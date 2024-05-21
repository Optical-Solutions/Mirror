--------------------------------------------------------
--  DDL for Procedure THIS_MERCH_DATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "ERIC"."THIS_MERCH_DATE" (
  p_merch_year OUT NUMBER,
  p_merch_period OUT NUMBER,
  p_merch_week OUT NUMBER,
  p_date IN DATE DEFAULT sysdate
  )
AS
/*
  Return the merch year, period, and week for the given date, defaulting to current date/time
*/
BEGIN
SELECT merchandising_year, merchandising_period, merchandising_week
INTO p_merch_year, p_merch_period, p_merch_week
FROM (
  SELECT merchandising_year, merchandising_period, merchandising_week
  FROM
  merchandising_calendars
  WHERE
  business_unit_id = 30
  AND week_ending_date <= p_date
  ORDER BY merchandising_year DESC, merchandising_week DESC
) WHERE rownum = 1;
END this_merch_date;

/
