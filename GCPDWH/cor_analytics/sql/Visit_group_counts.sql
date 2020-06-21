SELECT
    VISIT_GROUP_COUNT,
    COUNT(*)
FROM (
    SELECT
        CASE
        WHEN VISIT_COUNT = 1 THEN '1 VISIT'
        ELSE '>1 VISIT'
        END VISIT_GROUP_COUNT
    FROM (
        SELECT
            CUST_ID,
            MEMBER_NUM,
            COUNT(*) VISIT_COUNT
        FROM (
            SELECT DISTINCT 
                CUST_ID,
                MEMBER_NUM,
                INVOICE_DATETIME
            FROM `COR_ANALYTICS.NAPA_TRACKS`
            WHERE UPPER(FACILITY) ='BROKAW' 
        )
        GROUP BY
        CUST_ID,
        MEMBER_NUM 
    ) 
)
GROUP BY
    VISIT_GROUP_COUNT