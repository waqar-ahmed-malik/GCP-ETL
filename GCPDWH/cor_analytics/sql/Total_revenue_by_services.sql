--SERVICES WISE TOTAL REVENUE AND REPAIR ORDER COUNTS

SELECT
    DESCRIPT,
    SUM(TOTAL) REV,
    COUNT(*) RO_COUNT
FROM `COR_ANALYTICS.RO_WRITER_HRO` HRO
INNER JOIN (
    SELECT DISTINCT
        LOCATION_NO,
        RO_NO,
        RO_DATE,
        HLABOR_ID,
        CATEGORY
    FROM `COR_ANALYTICS.RO_WRITER_HLABOR`
) HLABOR
ON HRO.RO_NO = HLABOR.RO_NO
AND HRO.RO_DATE=HLABOR.RO_DATE
INNER JOIN `COR_ANALYTICS.RO_WRITER_CATEGORY` C
ON C.CATEGORY = HLABOR.CATEGORY
AND C.LOCATION_NO = HLABOR.LOCATION_NO
GROUP BY
    DESCRIPT
ORDER BY
    REV DESC