update  LANDING.STAGE_MEMBERSHIP_BILL set
    FILE_GENERATED='Y'
    WHERE EVENT_TYPE='BILL1'
    AND cast(UPDATE_DTTIME as date)=CURRENT_DATE()
    AND FILE_GENERATED='N';
