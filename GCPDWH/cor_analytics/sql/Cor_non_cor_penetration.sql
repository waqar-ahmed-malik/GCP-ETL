SELECT
    GENDER_CD,
    AGE_GRP,
    COUNT(COR)COR,
    COUNT(INSURANCE)INS,
    COUNT(MEMBER)AS MEM,
    COUNT(COR)/COUNT(MEMBER)*100 AS COR_PENETRATION,
    COUNT(INSURANCE)/COUNT(MEMBER)*100 AS INSURANCE_PENETRATION
FROM (
    SELECT *
    FROM (
        SELECT
            *,
            CASE
            WHEN AGE_YEAR BETWEEN 95 AND 109 THEN 'THE GREATEST GENERATION'
            WHEN AGE_YEAR BETWEEN 74
            AND 94 THEN 'THE SILENT GENERATION'
            WHEN AGE_YEAR BETWEEN 54 AND 74 THEN 'BABY BOOMERS'
            WHEN AGE_YEAR BETWEEN 39
            AND 53 THEN 'GEN X'
            WHEN AGE_YEAR BETWEEN 24 AND 38 THEN 'MILLENNIALS'
            WHEN AGE_YEAR BETWEEN 3
            AND 23 THEN 'GEN Z'
            END AS AGE_GRP
        FROM (
            SELECT
                DISTINCT MEMBER_NUM,
                BIRTH_DT,
                DATE_DIFF(CURRENT_DATE(),BIRTH_DT, YEAR) AS AGE_YEAR,
                GENDER_CD,
                MARITAL_STATUS_CD,
                BILLING_ADDRESS_LINE1,
                BILLING_CITY,
                BILLING_STATE,
                BILLING_ZIP,
                COR_FIRST_NM,
                COR_LAST_NM,
                COR,
                'MEMBER' AS MEMBER
            FROM `COR_ANALYTICS.MEMBERSHIP_CUSTOMER_DIM`
            LEFT JOIN (
                SELECT
                    DISTINCT MEMBER_NUM AS NUMBER,
                    CUST_ID AS COR_CUST_ID,
                    FIRST_NM AS COR_FIRST_NM,
                    LAST_NM AS COR_LAST_NM,
                    'COR' AS COR
                FROM `COR_ANALYTICS.NAPA_TRACKS`
                WHERE
                    MEMBER_NUM IS NOT NULL
                )
            ON MEMBER_NUM=NUMBER
            )
    LEFT JOIN (
    SELECT
    AGMT_NUM,
    MEMBER_NUM AS INS_MEM,
    'AUTOPOLICY' AS INSURANCE
    FROM (
    SELECT
      DISTINCT AGMT_NUM,
      MEMBER_NUM
    FROM
      `aaadata-181822.COR_ANALYTICS.INSURANCE_CUSTOMER_DIM`
    WHERE
      POLICY_STATUS ='A')
    INNER JOIN (
    SELECT
      AGMT_NUM AS AGREE
    FROM
      `COR_ANALYTICS.INSURANCE_DIM`
    WHERE
      PRODUCT_TYPE= 'Auto'
      )
    ON AGMT_NUM=AGREE
    )
    ON MEMBER_NUM=INS_MEM
        )
    )
GROUP BY
    GENDER_CD,
    AGE_GRP
HAVING
    GENDER_CD IS NOT NULL
    AND GENDER_CD !='U'
    AND AGE_GRP IS NOT NULL