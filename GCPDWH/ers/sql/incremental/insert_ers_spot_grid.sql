select 
TO_CHAR(translate(G.ID, chr(10)||chr(11)||chr(13),' ')) AS GRID_ID, 
TO_CHAR(translate(G.GRID_NAME, chr(10)||chr(11)||chr(13),' ')) AS GRID_NAME, 
TO_CHAR(translate(G.GRID_DESCRIPTION, '|','/')) AS GRID_DESCRIPTION, 
TO_CHAR(translate(G.GRID_GROUP_ID, chr(10)||chr(11)||chr(13),' ')) AS GRID_GROUP_ID, 
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,1) AS GEOMETRY1,
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,4001) AS GEOMETRY2,
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,8001) AS GEOMETRY3,
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,12001) AS GEOMETRY4,
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,16001) AS GEOMETRY5,
dbms_lob.substr(SDO_CS.TRANSFORM(G.GEOMETRY, 8199).Get_WKT(),4000,20001) AS GEOMETRY6,
TO_CHAR(LAST_MODIFIED,'YYYY-MM-DD HH24:MI:SS') as LAST_MODIFIED,
TO_CHAR(translate(G.PROMOTE_TASK_ID, chr(10)||chr(11)||chr(13),' ')) AS PROMOTE_TASK_ID, 
TO_CHAR(translate(G.REVERT_TASK_ID, chr(10)||chr(11)||chr(13),' ')) AS REVERT_TASK_ID, 
TO_CHAR(translate(G.DEV_ID, chr(10)||chr(11)||chr(13),' ')) AS DEV_ID, 
TO_CHAR(translate(G.TO_DELETE, chr(10)||chr(11)||chr(13),' ')) AS TO_DELETE,
'jobrunid' AS JOB_RUN_ID,
'jobname' AS CREATED_BY,
'ers' AS SOURCE_SYSTEM_CD
from NCA_SPOT.GRID G
WHERE LAST_MODIFIED >= TO_DATE('incr_date_spot_grid','yyyy-mm-dd hh24:mi:ss')