WITH STEP1 AS (
    SELECT
        FACILITY,
        AVG(TOTAL) AVG_PURCHASE_VAL
    FROM (
        SELECT
            CUST_ID,
            INVOICE_DT,
            RO_NUM,
            FACILITY,
            CAST(TOTAL AS FLOAT64) TOTAL
        FROM `COR_ANALYTICS.WORK_NAPA`
        WHERE
            PARSE_DATETIME('%m/%d/%Y %H:%M',
            INVOICE_DT) >= '2018-01-01'
        GROUP BY
            CUST_ID,
            INVOICE_DT,
            RO_NUM,
            FACILITY,
            TOTAL
    )
    GROUP BY
        FACILITY
),
STEP2 AS (
    SELECT
        FACILITY,
        PURCHASES/CUSTOMERS AVG_PURCH_FREQ
    FROM (
        SELECT
            FACILITY,
            COUNT(*) PURCHASES,
            COUNT(DISTINCT CUST_ID) CUSTOMERS
        FROM (
            SELECT
                CUST_ID,
                INVOICE_DT,
                RO_NUM,
                FACILITY,
                CAST(TOTAL AS FLOAT64) TOTAL
            FROM `COR_ANALYTICS.WORK_NAPA`
            WHERE
                PARSE_DATETIME('%m/%d/%Y %H:%M',
                INVOICE_DT) >= '2018-01-01'
            GROUP BY
                CUST_ID,
                INVOICE_DT,
                RO_NUM,
                FACILITY,
                TOTAL
        )
        GROUP BY
        FACILITY
    )
),
STEP3 AS (
    SELECT
        FACILITY,
        AVG(DURATION)/365 REPEAT_DURATION
    FROM (
        SELECT
            FACILITY,
            CUST_ID,
            DATETIME_DIFF( MAX(PARSE_DATETIME('%m/%d/%Y %H:%M', INVOICE_DT)), MIN(PARSE_DATETIME('%m/%d/%Y %H:%M', INVOICE_DT)), DAY) DURATION
        FROM `COR_ANALYTICS.WORK_NAPA`
        GROUP BY
            FACILITY,
            CUST_ID
    )
    WHERE
        DURATION > 0
    GROUP BY
        FACILITY
)
SELECT
    FACILITY,
    AVG_PURCH_FREQ*AVG_PURCHASE_VAL*REPEAT_DURATION CUST_LIFETIME_VAL
FROM (
    SELECT
        STEP2.*,
        STEP1.AVG_PURCHASE_VAL,
        STEP3.REPEAT_DURATION
    FROM STEP1
    INNER JOIN STEP2
    ON STEP1.FACILITY = STEP2.FACILITY
    INNER JOIN STEP3
    ON STEP1.FACILITY = STEP3.FACILITY
)