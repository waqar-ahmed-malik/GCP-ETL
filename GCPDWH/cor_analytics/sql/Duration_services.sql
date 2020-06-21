SELECT DISTINCT
    LOCATIONNO,
    RO_NO,
    DURATION_HOUR,
    TOTAL,
    CASE
        WHEN DURATION_HOUR <15 THEN 'LOW_DURATION'
        WHEN DURATION_HOUR <18 THEN 'MEDIUM_DURATION'
        WHEN DURATION_HOUR <58 THEN 'HIGH_DURATION'
        ELSE 'EX_HIGH_DURATION'
        END AS DURATION_CAT,
    COUNT(DESC_LINES)SERVICE_COUNT,
    COUNT(CATEGORY)CATEGORY_COUNT
FROM (
    SELECT
        LOCATIONNO,
        RO_NO,
        CUST_NO,
        TOTAL,
        LABOR.DESC_LINES,
        LABOR.T_COST,
        LABOR.CATEGORY,
        RO_DATE,
        CLOSED_DTTIME,
        LABOR.TIME,
        DATETIME_DIFF( CLOSED_DTTIME, DATETIME(RO_DATE), HOUR) DURATION_HOUR,
        HOURS
    FROM `COR_ANALYTICS.RO_WRITER_HRO`
    INNER JOIN (
        SELECT
            RO_NO AS R_NO,
            TRIM(DESC_LINES)DESC_LINES,
            TIME,
            T_COST,
            CATEGORY,
            LOCATION_NO,
            HLABOR_ID
        FROM `COR_ANALYTICS.RO_WRITER_HLABOR`
        ) LABOR
    ON RO_NO =R_NO
    AND LOCATIONNO = LOCATION_NO
    )
GROUP BY
    LOCATIONNO,
    RO_NO,
    DURATION_HOUR,
    TOTAL