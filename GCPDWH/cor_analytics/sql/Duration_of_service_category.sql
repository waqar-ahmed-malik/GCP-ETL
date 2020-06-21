SELECT
    APPROX_QUANTILES(DURATION_HOUR, 4) AS percentiles,
    AVG(TOTAL)
FROM (
    SELECT
        DISTINCT TOTAL,
        DURATION_HOUR,
        LOCATIONNO
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
                TRIM(DESC_LINES) DESC_LINES,
                LOCATION_NO,
                TIME,
                T_COST,
                CATEGORY,
                HLABOR_ID
            FROM `COR_ANALYTICS.RO_WRITER_HLABOR` )LABOR
        ON RO_NO = R_NO
        AND LOCATIONNO = LOCATION_NO
        )
    )