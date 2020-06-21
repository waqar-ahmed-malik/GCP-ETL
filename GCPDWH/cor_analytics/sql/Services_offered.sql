--ro_writer
SELECT
    DESCRIPT,
    COUNT(*)
FROM (
    SELECT
        C.DESCRIPT,
        HRO.RO_NO,
        HRO.LOCATIONNO,
        COUNT(*)
    FROM COR_ANALYTICS.RO_WRITER_HRO HRO
    LEFT OUTER JOIN COR_ANALYTICS.RO_WRITER_HLABOR HL
    ON HRO.RO_NO = HL.RO_NO
    AND HRO.LOCATIONNO = HL.LOCATION_NO
    INNER JOIN COR_ANALYTICS.RO_WRITER_CATEGORY C
    ON HL.CATEGORY = C.CATEGORY
    GROUP BY
        C.DESCRIPT,
        HRO.RO_NO,
        HRO.LOCATIONNO
)
GROUP BY
    DESCRIPT