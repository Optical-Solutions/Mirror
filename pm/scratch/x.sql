SELECT
(select 1 from dual where  REGEXP_LIKE(s.description, '\\,|\\"') ) punct_flg,
 m.merchandising_year,
 m.merchandising_period,
 m.merchandising_week,
 to_char(m.week_ending_date, 'yyyymmdd') week_ending_date,
 SD.SITE_ID,
 ss.name,
 sd.bar_code_id,
 S.DESCRIPTION,
 v.department_id,
 v.dept_name,
 v.class_id,
 v.class_descr,
 v.sub_class_id,
 v.sub_class_descr,
 SUM(SD.QTy) qty,
 sum(sd.extension_amount) extension_amount

FROM 
 SALE_DETAILS   SD 
 
 join STYLES S on (SD.BUSINESS_UNIT_ID = S.BUSINESS_UNIT_ID AND
                   SD.STYLE_ID = S.STYLE_ID)
 
 join BAR_CODES B on (SD.BUSINESS_UNIT_ID = B.BUSINESS_UNIT_ID AND
                      SD.bar_code_sub_type = B.sub_type AND
                      SD.BAR_CODE_ID = B.BAR_CODE_ID)
 
 join QSENSE.V_DEPT_CLASS_SUBCLASS2 V on(s.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID AND
                                         S.SECTION_ID = V.SECTION_ID)
 
 /*MERCHANDINSG CAL AND LAST CLOSE WEEK */                                
 join (select merchandising_period, merchandising_year, merchandising_week,
       (week_ending_date - 6) week_start_date, week_ending_date from 
        (select * from merchandising_calendars where date_closed is not null 
         order by date_closed desc)
      where rownum = 1) m on (1 = 1)
 
 join sites ss on (sd.business_unit_id = ss.business_unit_id and
                   sd.site_id = ss.site_id)
                
WHERE 
 SD.BUSINESS_UNIT_ID = 30 AND
 sd.sale_date > week_start_date - 1 and 
 SD.SALE_DATE BETWEEN m.week_start_date and week_ending_date and
 sd.sub_type = 'ITEM' AND
 v.inventory = 'Y'
GROUP BY
 m.merchandising_year,  m.merchandising_period,
 SD.SITE_ID,  ss.name,  sd.bar_code_id,  S.DESCRIPTION,  v.department_id,
 v.dept_name,  v.class_id,  v.class_descr,  v.sub_class_id,  v.sub_class_descr,
 M.MERCHANDISING_WEEK, M.WEEK_ENDING_DATE
