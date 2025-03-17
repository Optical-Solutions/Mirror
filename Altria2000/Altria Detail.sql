select '64549' || '|' || TO_CHAR(
     m.week_ending_date, 'YYYYMMDD') || '|' || TO_CHAR(SD.SALE_DATE, 'YYYYMMDD') || '|' || TO_CHAR(SA.SALE_DATE_TIME, 'HH24:MI:SS') || '|' || SA.SITE_ID || SA.SLIP_NO || SA.REGISTER_ID || '|' || SA.SITE_ID || '|' || A.NAME || '|' || A.ADDRESS_2 || '|' || A.CITY || '|' || A.STATE_ID || '|' || A.ZIP_CODE || '|' || V.SUB_CLASS_DESCR || '|' || CV.DESCRIPTION || '|' || SD.STYLE_ID || '|' || SD.BAR_CODE_ID || '|' || S.DESCRIPTION || '|' || CASE
          WHEN v.department_id = '0991'
   
          and V.CLASS_ID = '1100'
          AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1100'
          AND STYLE_TYPE = 'SINGLE' THEN 'PACK'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1200'
          AND STYLE_TYPE = 'MULTI' THEN 'SLEEVE'
          WHEN v.department_id = '0991'
          and V.CLASS_ID = '1200'
          AND STYLE_TYPE = 'SINGLE' THEN 'CAN'
          wHEN V.DEPARTMENT_ID = '0992'
          AND V.CLASS_ID = '2000'
          AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
          WHEN V.DEPARTMENT_ID = '0992'
          AND V.CLASS_ID = '2000'
          AND STYLE_TYPE = 'SINGLE' THEN 'EA'
          ELSE 'EA'
     END || '|' || SD.QTY || '|' || CASE
          WHEN STYLE_TYPE = 'MULTI' THEN (
               SELECT distinct TO_CHAR(X.MULTI_QTY)
               FROM STYLES X
               WHERE BUSINESS_UNIT_ID = 30
                    AND X.MULTI_STYLE_ID = S.STYLE_ID
          )
          else '1'
     END || '|' || NULL || '|' || --Multi Unit Indicator
     NULL || '|' || --Multi Unit Required Quantity
     NULL || '|' || --Multi Unit Discount Amount
     NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || SD.EXTENSION_AMOUNT || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || NULL || '|' || 'In Store' || '|' || NULL || '|' || NULL || '|' || NULL
FROM STYLES S,
     SALES SA,
     SALE_DETAILS SD,
     rsg.SITES_ALTRIA @mc2p A,
     V_DEPT_CLASS_SUBCLASS V,
     CHARACTERISTIC_VALUES cv,
     STYLE_CHARACTERISTICS SC,
     MERCH_CAL_MIKE_WEEK M
WHERE S.BUSINESS_UNIT_iD = 30
     AND S.BUSINESS_UNIT_ID = SA.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = SD.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = A.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID
     AND S.BUSINESS_UNIT_ID = CV.BUSINESS_UNIT_iD
     AND S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_iD
     AND S.BUSINESS_UNIT_ID = M.BUSINESS_UNIT_ID
     AND S.STYle_ID = SD.STYLE_ID
     AND SA.SITE_ID = SD.SITE_ID
     AND SA.SALE_DATE = SD.SALE_DATE
     AND SA.SLIP_NO = SD.SLIP_NO
     AND SA.REGISTER_ID = SD.REGISTER_ID
     AND SA.SITE_ID = A.SITE_ID
     AND S.SECTION_ID = V.SECTION_ID
     AND S.STYLE_ID = SC.STYLE_ID
     AND SC.CHARACTERISTIC_TYPE_ID = CV.CHARACTERISTIC_TYPE_ID
     AND CV.CHARACTERISTIC_TYPE_ID = 'BRAND'
     AND SC.CHARACTERISTIC_VALUE_ID = CV.CHARACTERISTIC_VALUE_ID
     AND Sa.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE
     AND (
          (
               (
                    V.DEPARTMENT_ID = '0991'
                    and v.class_id IN ('1100', '1200')
               )
               or (
                    v.department_id = '0992'
                    and v.class_id = '2000'
                    and style_type != 'REGULAR'
               )
          )
     )
     and sa.sale_date between trunc(sysdate -8) and trunc(sysdate -2)