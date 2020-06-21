SELECT
TO_CHAR(translate(VEH_MAKE,chr(10)||chr(11)||chr(13),' ')) as VEH_MAKE,
TO_CHAR(translate(VEH_MODEL,chr(10)||chr(11)||chr(13),' ')) as VEH_MODEL,
TO_CHAR(translate(VEH_YEAR,chr(10)||chr(11)||chr(13),' ')) as VEH_YEAR,
TO_CHAR(translate(FLATBED_INDIC,chr(10)||chr(11)||chr(13),' ')) as FLATBED_INDIC,
TO_CHAR(translate(FOUR_WHEEL_DRIVE,chr(10)||chr(11)||chr(13),' ')) as FOUR_WHEEL_DRIVE,
TO_CHAR(translate(FOREIGN_VEHICLE,chr(10)||chr(11)||chr(13),' ')) as FOREIGN_VEHICLE,
TO_CHAR(translate(SCRIPT_ID,chr(10)||chr(11)||chr(13),' ')) as SCRIPT_ID,
TO_CHAR(translate(ELECTRIC_POWERED,chr(10)||chr(11)||chr(13),' ')) as ELECTRIC_POWERED,
TO_CHAR(translate(REC_VEHICLE,chr(10)||chr(11)||chr(13),' ')) as REC_VEHICLE,
TO_CHAR(translate(LOCKSMITH_REQ,chr(10)||chr(11)||chr(13),' ')) as LOCKSMITH_REQ,
TO_CHAR(translate(AXLE_TYPE,chr(10)||chr(11)||chr(13),' ')) as AXLE_TYPE,
TO_CHAR(translate(PROC_NUMBER,chr(10)||chr(11)||chr(13),' ')) as PROC_NUMBER,
TO_CHAR(translate(BASE_VEHICLE_ID,chr(10)||chr(11)||chr(13),' ')) as BASE_VEHICLE_ID,
TO_CHAR(translate(VEHICLE_ID,chr(10)||chr(11)||chr(13),' ')) as VEHICLE_ID,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
FROM NCA_CAD.VEHICLE_DATA