-- EFFICIENCY OF OPERATION
  -- I HAVE USED AVG TOTAL PER LOCATION /AVG HOURS SPENT PER LOCATION . THIS WOULD GIVE US HOURLY AVG REVENUE
  -- I HAVE THEN MULTIPLIED TO HOURLY AVG REVENUE WITH THE ACTUAL HOUR PER REPAIR ORDER TO GET THE EXPECTED REVENUE
  -- I HAVE THEN DIVIDED TOTAL PER REPAIR ORDER  FROM EXPECTED REVENUE TO GET THE EFFICIENCY
SELECT
    LOC_NO,
    RO_NO,
    DURATION_HOUR,
    TOTAL,
    SERVICE_COUNT,
    CATEGORY_COUNT,
    AVG_TOTAL_PER_HOUR,
    EXPECTED_TOTAL,
    (TOTAL/EXPECTED_TOTAL )*100 AS ACHIEVED_EFFICIENCY
FROM (
    SELECT
        *,
        DURATION_HOUR*AVG_TOTAL_PER_HOUR AS EXPECTED_TOTAL
    FROM (
        SELECT
            WHOLE.*,
            AVG_TOTAL_PER_HOUR
        FROM (
            SELECT DISTINCT
                LOC_NO,
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
                    LOCATIONNO AS LOC_NO,
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
                ON RO_NO = R_NO
                AND LOCATIONNO = LOCATION_NO
            )
            WHERE DURATION_HOUR >0
            GROUP BY
                LOC_NO,
                RO_NO,
                DURATION_HOUR,
                TOTAL
        ) AS WHOLE
        INNER JOIN (
            SELECT
                AVG(TOTAL)/AVG(DATETIME_DIFF( CLOSED_DTTIME, DATETIME(RO_DATE),HOUR)) AS AVG_TOTAL_PER_HOUR,
                LOCATIONNO AS LOCA_NO
            FROM `COR_ANALYTICS.RO_WRITER_HRO`
            GROUP BY
                LOCATIONNO
        )
        ON LOC_NO = LOCA_NO
    )
)