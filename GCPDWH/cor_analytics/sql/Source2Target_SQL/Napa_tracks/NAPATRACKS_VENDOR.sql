SELECT
  CAST (VENDID AS INT64) VEND_ID,
  VENDNAME AS VEND_NAME,
  ADDRESS1,
  ADDRESS2,
  CITY,
  STATE,
  ZIP,
  PHONENUM AS PHONE_NUM,
  TAMS,
  TAMSACCTID AS TAMS_ACCT_ID,
  TAMSCUSTID AS TAMS_CUST_ID,
  APACCT AS AP_ACCT,
  NAPAYN AS NAPA_YN,
  MODEM AS MODEM,
  CAST(PRIORITY AS INT64) PRIORITY,
  ACTIVEFLAG AS ACTIVE_FLAG,
  REMOTETRACSYN AS REMOTE_RACS_YN,
  FAXNUM AS FAX_NUM,
  PHONENUM2 AS PHONE_NUM2,
  STOREID AS STORE_ID,
  DIALONNETERRORYN AS DIAL_ON_NET_ERROR_YN,
  NOTES,
  EMAIL,
  CONTACT,
  ALTVENDID AS ALTVEND_ID,
  ACCOUNTPAYTYPE AS ACCOUNT_PAY_TYPE,
  DCDIRECTENABLEDYN AS DC_DIRECT_ENABLED_YN,
  ACONNEXYN AS ACONNEX_YN,
  ISWORLDPACUSESPEEDDIALYN AS ISWORLDPAC_USE_SPEED_DIALYN,
  SHOWALTPARTS AS SHOW_ALT_PARTS
FROM
  `aaadata-181822.COR_ANALYTICS.STG_NAPATRACKS_VENDOR`