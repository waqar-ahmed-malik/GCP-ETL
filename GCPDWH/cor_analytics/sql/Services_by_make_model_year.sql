SELECT
    MAKE,
    MODEL,
    YEAR,
    DESCRIPT,
    COUNT(*)
FROM (
    SELECT DISTINCT
        CUST_NO,
        MAKE,
        MODEL,
        YEAR,
        MILEAGE,
        TOTAL,
        C.DESCRIPT,
        HL.*
    FROM `COR_ANALYTICS.RO_WRITER_HRO` HRO
    INNER JOIN `COR_ANALYTICS.RO_WRITER_HLABOR` HL
    ON HRO.RO_NO = HL.RO_NO
    AND HRO.LOCATIONNO=HL.LOCATION_NO
    INNER JOIN `COR_ANALYTICS.RO_WRITER_CATEGORY` C
    ON C.CATEGORY = HL.CATEGORY
    AND HRO.LOCATIONNO=HL.LOCATION_NO
)
WHERE
    MAKE IS NOT NULL
    AND MODEL IS NOT NULL
GROUP BY
    MAKE,
    MODEL,
    YEAR,
    DESCRIPT
ORDER BY
    COUNT(*) DESC