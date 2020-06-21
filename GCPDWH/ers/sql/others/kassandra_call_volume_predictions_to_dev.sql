DELETE FROM `ers-optima.MWGRoadService.CALL_VOLUME_PREDICTIONS`
WHERE DATE(insert_timestamp) in (SELECT DATE(max(insert_timestamp)) FROM `aaa-mwg-ersprod.MWGRoadService.CALL_VOLUME_PREDICTIONS`);


INSERT INTO `ers-optima.MWGRoadService.CALL_VOLUME_PREDICTIONS`(date
,trip_hour_of_day
,grid
,lat
,long
,truck_type
,predicted_count
,insert_timestamp
)
SELECT date
,trip_hour_of_day
,grid
,lat
,long
,truck_type
,predicted_count
,insert_timestamp
FROM  `aaa-mwg-ersprod.MWGRoadService.CALL_VOLUME_PREDICTIONS`
WHERE DATE(insert_timestamp) in (SELECT DATE(max(insert_timestamp)) FROM `aaa-mwg-ersprod.MWGRoadService.CALL_VOLUME_PREDICTIONS`);