select '64549' || '|' || (
        select TO_CHAR(m.week_ending_date, 'YYYYMMDD')
        from MERCH_CAL_MIKE_WEEK M
        where Sa.SALE_DATE BETWEEN M.WEEK_STARTING_DATE AND M.WEEK_ENDING_DATE
    ) || '|' || TO_CHAR(SD.SALE_DATE, 'YYYYMMDD') || '|' || TO_CHAR(SA.SALE_DATE_TIME, 'HH24:MI:SS') || '|' || SA.SITE_ID || SA.SLIP_NO || SA.REGISTER_ID || '|' || SA.SITE_ID || '|' || A.NAME || '|' || A.ADDRESS_2 || '|' || A.CITY || '|' || A.STATE_ID || '|' || A.ZIP_CODE || '|' || V.SUB_CLASS_DESCR || '|' || CV.DESCRIPTION || '|' || SD.STYLE_ID || '|' || SD.BAR_CODE_ID || '|' || S.DESCRIPTION || '|' || CASE
        WHEN V.DEPARTMENT_ID = '0992'
        AND V.CLASS_ID = '2000' THEN 'DEPARTMENT 0992:CLASS 2000'
        WHEN V.CLASS_ID = '1100'
        AND STYLE_TYPE = 'MULTI' THEN 'CARTON'
        WHEN V.CLASS_ID = '1100'
        AND STYLE_TYPE = 'SINGLE' THEN 'PACK'
        WHEN V.CLASS_ID = '1200'
        AND STYLE_TYPE = 'MULTI' THEN 'SLEEVE'
        WHEN V.CLASS_ID = '1200'
        AND STYLE_TYPE = 'SINGLE' THEN 'CAN'
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
FROM SALES SA
    join SALE_DETAILS SD on (
        SA.SITE_ID = SD.SITE_ID
        AND SA.SALE_DATE = SD.SALE_DATE
        AND SA.SLIP_NO = SD.SLIP_NO
        AND SA.REGISTER_ID = SD.REGISTER_ID
    )
    join STYLES S on (
        s.business_unit_id = sa.business_unit_id
        and s.style_id = sd.style_id
    )
    join rsg.SITES_ALTRIA @mc2p A on (
        sa.business_unit_id = a.business_unit_id
        and sa.site_id = a.site_id
    )
    JOIN V_DEPT_CLASS_SUBCLASS V ON S.BUSINESS_UNIT_ID = V.BUSINESS_UNIT_ID
    AND S.SECTION_ID = V.SECTION_ID
    AND (
        (
            V.DEPARTMENT_ID = '0991'
            AND V.CLASS_ID IN ('1100', '1200')
        )
        OR (
            V.DEPARTMENT_ID = '0992'
            AND V.CLASS_ID = '2000'
        )
    )
    JOIN STYLE_CHARACTERISTICS SC ON S.BUSINESS_UNIT_ID = SC.BUSINESS_UNIT_ID
    AND S.STYLE_ID = SC.STYLE_ID
    join CHARACTERISTIC_VALUES cv on (
        Sc.business_unit_id = cv.business_unit_id
        and SC.CHARACTERISTIC_TYPE_ID = CV.CHARACTERISTIC_TYPE_ID
        AND CV.CHARACTERISTIC_TYPE_ID = 'BRAND'
        AND SC.CHARACTERISTIC_VALUE_ID = CV.CHARACTERISTIC_VALUE_ID
    )
WHERE 1 = 1
    AND sa.sale_date between trunc(sysdate -(dow + 7)) and trunc(sysdate -(dow + 1))
ORDER BY SA.SITE_ID,
    SA.SLIP_NO,
    SA.REGISTER_ID