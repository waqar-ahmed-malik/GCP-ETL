select 
TO_CHAR(translate(G.ID, chr(10)||chr(11)||chr(13),' ')) AS ID, 
TO_CHAR(translate(G.GRID_ID, chr(10)||chr(11)||chr(13),' ')) AS GRID_ID, 
TO_CHAR(translate(G.BACKUP_LEVEL, chr(10)||chr(11)||chr(13),' ')) AS BACKUP_LEVEL, 
TO_CHAR(translate(G.FACILITY_ID, chr(10)||chr(11)||chr(13),' ')) AS FACILITY_ID, 
TO_CHAR(translate(G.SPOT_TYPE, chr(10)||chr(11)||chr(13),' ')) AS SPOT_TYPE, 
TO_CHAR(translate(G.START_TIME, chr(10)||chr(11)||chr(13),' ')) AS START_TIME, 
TO_CHAR(translate(G.END_TIME, chr(10)||chr(11)||chr(13),' ')) AS END_TIME, 
TO_CHAR(translate(G.TRUCK_ZONE, chr(10)||chr(11)||chr(13),' ')) AS TRUCK_ZONE, 
TO_CHAR(translate(G.STREET_NAME, chr(10)||chr(11)||chr(13),' ')) AS STREET_NAME, 
TO_CHAR(translate(G.CALL_PERCENTAGE, chr(10)||chr(11)||chr(13),' ')) AS CALL_PERCENTAGE,
TO_CHAR(translate(G.STREET_SIDE, chr(10)||chr(11)||chr(13),' ')) AS STREET_SIDE
from NCA_SPOT.GRID_ASSIGNMENTS G