DELETE FROM LANDING.WORK_CTS_COMPLAINT_DIM WHERE TRUE
UNION ALL
DELETE FROM LANDING.WORK_CTS_COMPLAINT_HISTORY_DIM WHERE TRUE
UNION ALL
DELETE FROM LANDING.WORK_CTS_INCIDENT_FACT WHERE TRUE
UNION ALL
DELETE FROM LANDING.WORK_CTS_INCIDENT_HISTORY_FACT WHERE TRUE
UNION ALL
DELETE FROM LANDING.WORK_CTS_INCIDENT_ROUTING_LIST_FACT WHERE TRUE