select 
  Cost_Center_CD,
  cast('{{ ds_nodash }}' as int64) as fileid, 
  Cost_Center_Desc
from LANDING.WD_COSTCENTER_RAW_{{ ds_nodash }}
where 1=1
  and REGEXP_CONTAINS(Cost_Center_CD,r'(\d{4})')
order by 1,2 DESC;