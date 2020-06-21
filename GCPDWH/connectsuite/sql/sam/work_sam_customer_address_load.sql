SELECT ADDRESS_ID,
CUSTOMER_ID,
ADDRESS_TYPE,
ADDRESS1,
ADDRESS2,
CITY,
STATE,
ZIP,
COUNTRY,
COUNTRY_NM,
COUNTRY_CD,
AUDIT_DT FROM  (
SELECT 
CONVERT(char, a.addressid ) AS ADDRESS_ID,
CONVERT(char, a.customerid ) AS CUSTOMER_ID,
CONVERT(char, a.addresstype ) AS ADDRESS_TYPE,
a.address1 AS ADDRESS1,
a.address2 AS ADDRESS2,
a.city AS CITY,
a.state AS STATE,
CONVERT(char, a.zip ) AS ZIP,
a.country AS COUNTRY,
a.countyname AS COUNTRY_NM,
CONVERT(char, a.countycode) AS COUNTRY_CD,
CONVERT(char,b.audit_dt,120 ) as AUDIT_DT,
ROW_NUMBER() OVER  (PARTITION BY a.addressid order by b.audit_dt DESC ) RN
from
--"v_database_name".dbo.addresses a, "v_database_name".dbo.addresses_audit b
mrm_sam_prd.dbo.addresses a
INNER JOIN
 (select * from mrm_sam_prd.dbo.customers_audit b WHERE
b.audit_dt >= CONVERT(DATE, GETDATE() - 2)) b
on a.customerid=b.customerid
) a
WHERE a.RN=1
