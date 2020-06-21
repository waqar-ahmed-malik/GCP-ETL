SELECT
    COUNT(*) AS FREQ_OF_CAR_FOR_SERVICE,
    HRO.MAKE,
    HRO.YEAR,
    C.DESCRIPT LABOR_CATEGORY
FROM `COR_ANALYTICS.RO_WRITER_HRO` HRO
INNER JOIN `COR_ANALYTICS.RO_WRITER_HLABOR` LBR
ON HRO.RO_NO = LBR.RO_NO
AND HRO.LOCATIONNO = LBR.LOCATION_NO
INNER JOIN `COR_ANALYTICS.RO_WRITER_CATEGORY` C
ON C.CATEGORY = LBR.CATEGORY
AND C.LOCATION_NO = LBR.LOCATION_NO
WHERE
    HRO.RO_DATE >= '2018-01-01'
    AND DESCRIPT != '**CUST COMMENTS'
GROUP BY
    HRO.MAKE,
    HRO.YEAR,
    C.DESCRIPT
ORDER BY
    FREQ_OF_CAR_FOR_SERVICE DESC