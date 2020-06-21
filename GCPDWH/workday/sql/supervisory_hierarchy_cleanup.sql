select 
	cast('{{ ds_nodash }}' as int64) as fileid, 
	* 
from LANDING.WD_SUPERVISORY_HIERARCHY_RAW_{{ ds_nodash }}
order by 4, 1 desc;