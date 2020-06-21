SELECT
    COUNT(*)REPEAT_CUST_TOTAL
FROM (
    SELECT
        CUST_ID,
        FIRST_NM,
        LAST_NM,
        MEMBER_NUM
    FROM (
        SELECT DISTINCT
            CUST_ID,
            FIRST_NM,
            LAST_NM,
            MEMBER_NUM,
            INVOICE_DATETIME
        FROM `COR_ANALYTICS.NAPA_TRACKS`
        WHERE
            UPPER(FACILITY) ='BROKAW'
    )
    GROUP BY
        CUST_ID,
        FIRST_NM,
        LAST_NM,
        MEMBER_NUM
    HAVING
        COUNT(*) >1
)