DELETE FROM `ers-optima.MWGRoadService.ACTIVE_CALLS_ON_EVEN_DAYS`;


INSERT INTO `ers-optima.MWGRoadService.ACTIVE_CALLS_ON_EVEN_DAYS`(SC_ID
,SC_DT
,SC_FACILITY_ID
,SC_TRUCK_ID
,TRUCK_TYPE_CD
,TRUCK_TYPE
,TRUCK_DRIVER_STATUS_CD
,GPS_UPDATE_DTTIME
,ETA
,STATUS_CD
,MEMBERSHIP_NUM
,SC_PRIMARY_CONTACT_FIRST_NAME
,SC_PRIMARY_CONTACT_LAST_NAME
,SERVICE_VEHICLE_LATITUDE
,SERVICE_VEHICLE_LONGITUDE
,BREAKDOWN_LOCATION_LATITUDE
,BREAKDOWN_LOCATION_LONGITUDE
,LAST_UPDATE_DTTIME
)
SELECT SC_ID
,SC_DT
,SC_FACILITY_ID
,SC_TRUCK_ID
,TRUCK_TYPE_CD
,TRUCK_TYPE
,TRUCK_DRIVER_STATUS_CD
,GPS_UPDATE_DTTIME
,ETA
,STATUS_CD
,MEMBERSHIP_NUM
,SC_PRIMARY_CONTACT_FIRST_NAME
,SC_PRIMARY_CONTACT_LAST_NAME
,SERVICE_VEHICLE_LATITUDE
,SERVICE_VEHICLE_LONGITUDE
,BREAKDOWN_LOCATION_LATITUDE
,BREAKDOWN_LOCATION_LONGITUDE
,LAST_UPDATE_DTTIME
FROM  `aaa-mwg-ersprod.MWGRoadService.ACTIVE_CALLS_ON_EVEN_DAYS`
WHERE DATE(LAST_UPDATE_DTTIME) in (SELECT DATE(max(LAST_UPDATE_DTTIME)) FROM `aaa-mwg-ersprod.MWGRoadService.ACTIVE_CALLS_ON_EVEN_DAYS`);