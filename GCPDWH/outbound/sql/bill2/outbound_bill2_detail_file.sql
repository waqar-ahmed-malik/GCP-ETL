  --Bill1 sql query to be put here
SELECT
ARIA_ACCOUNT_ID AS ACCT_NO,	
MEMBER_NUM as MEMBERSHIP_NO,	
MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",MEMBER_EXPIRY_DT) AS MEMBER_EXPIRY_DT,
PLAN,
EVENT_TYPE,
FIRST_NAME,
MI,
LAST_NAME,
EMAIL,
PHONE,
ADDRESS1,
CITY,
STATE,
ZIP,
COUNTRY,
CC_LAST_4_DIGITS,
PAY_FAILED,
PROMOCODE1,
PROMO1_AMOUNT,
PROMOCODE2,
PROMO2_AMOUNT,
PROMOCODE3,
PROMO3_AMOUNT,
SEC_MEMBER_DATA_PRESENT,
ASSOC_MEMBER_DATA_PRESENT,
NUM_ASSOC_MEMBERS,
DONOR_MEMBER_DATA_PRESENT,
GROUP_DATA_PRESENT,
OTHER_INV_ITEM_DATA_PRESENT,
NUM_OTHER_INV_ITEMS,
PRIMARY_MEMBER_NUM AS PRIMARY_MEMBERSHIP_NO,
PRIMARY_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",PRIMARY_MEMBER_EXPIRY_DT) AS PRIMARY_MEMBER_EXPIRY_DT,
PRIMARY_MEMBER_FIRST_NAME,
PRIMARY_MEMBER_MI,
PRIMARY_MEMBER_LAST_NAME,
PRIMARY_MEMBER_FULL_NAME,
PRIMARY_MEMBER_LINE_ITEM_DES,
PRIMARY_MEMBER_LINE_ITEM_AMT,
PRIMARY_MEMBER_ALTERNATE_AMT,
SECONDARY_MEMBER_NUM SECONDARY_MEMBERSHIP_NO,
SECONDARY_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",SECONDARY_MEMBER_EXPIRY_DT) AS SECONDARY_MEMBER_EXPIRY_DT,
SECONDARY_MEMBER_FIRST_NAME,
SECONDARY_MEMBER_MI,
SECONDARY_MEMBER_LAST_NAME,
SECONDARY_MEMBER_FULL_NAME,
SECONDARY_MEMBER_LINE_ITEM_DES,
SECONDARY_MEMBER_LINE_ITEM_AMT,
SECONDARY_MEMBER_ALTERNATE_AMT,
ASSOC_1_MEMBER_NUM ASSOC_1_MEMBERSHIP_NO,
ASSOC_1_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_1_MEMBER_EXPIRY_DT) AS ASSOC_1_MEMBER_EXPIRY_DT,
ASSOC_1_FIRST_NAME,
ASSOC_1_MI,
ASSOC_1_LAST_NAME,
ASSOC_1_FULL_NAME,
ASSOC_1_LINE_ITEM_DES,
ASSOC_1_LINE_ITEM_AMT,
ASSOC_1_ALTERNATE_AMT,
ASSOC_2_MEMBER_NUM AS ASSOC_2_MEMBERSHIP_NO,
ASSOC_2_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_2_MEMBER_EXPIRY_DT) AS ASSOC_2_MEMBER_EXPIRY_DT,
ASSOC_2_FIRST_NAME,
ASSOC_2_MI,
ASSOC_2_LAST_NAME,
ASSOC_2_FULL_NAME,
ASSOC_2_LINE_ITEM_DES,
ASSOC_2_LINE_ITEM_AMT,
ASSOC_2_ALTERNATE_AMT,
ASSOC_3_MEMBER_NUM AS ASSOC_3_MEMBERSHIP_NO,
ASSOC_3_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_3_MEMBER_EXPIRY_DT) AS ASSOC_3_MEMBER_EXPIRY_DT,
ASSOC_3_FIRST_NAME,
ASSOC_3_MI,
ASSOC_3_LAST_NAME,
ASSOC_3_FULL_NAME,
ASSOC_3_LINE_ITEM_DES,
ASSOC_3_LINE_ITEM_AMT,
ASSOC_3_ALTERNATE_AMT,
ASSOC_4_MEMBER_NUM AS ASSOC_4_MEMBERSHIP_NO,
ASSOC_4_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_4_MEMBER_EXPIRY_DT) AS ASSOC_4_MEMBER_EXPIRY_DT,
ASSOC_4_FIRST_NAME,
ASSOC_4_MI,
ASSOC_4_LAST_NAME,
ASSOC_4_FULL_NAME,
ASSOC_4_LINE_ITEM_DES,
ASSOC_4_LINE_ITEM_AMT,
ASSOC_4_ALTERNATE_AMT,
ASSOC_5_MEMBER_NUM AS ASSOC_5_MEMBERSHIP_NO,
ASSOC_5_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_5_MEMBER_EXPIRY_DT) AS ASSOC_5_MEMBER_EXPIRY_DT,
ASSOC_5_FIRST_NAME,
ASSOC_5_MI,
ASSOC_5_LAST_NAME,
ASSOC_5_FULL_NAME,
ASSOC_5_LINE_ITEM_DES,
ASSOC_5_LINE_ITEM_AMT,
ASSOC_5_ALTERNATE_AMT,
ASSOC_6_MEMBER_NUM AS ASSOC_6_MEMBERSHIP_NO,
ASSOC_6_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_6_MEMBER_EXPIRY_DT) AS ASSOC_6_MEMBER_EXPIRY_DT,
ASSOC_6_FIRST_NAME,
ASSOC_6_MI,
ASSOC_6_LAST_NAME,
ASSOC_6_FULL_NAME,
ASSOC_6_LINE_ITEM_DES,
ASSOC_6_LINE_ITEM_AMT,
ASSOC_6_ALTERNATE_AMT,
ASSOC_7_MEMBER_NUM AS ASSOC_7_MEMBERSHIP_NO,
ASSOC_7_MEMBER_SINCE,
FORMAT_DATE("%Y-%m-%d",ASSOC_7_MEMBER_EXPIRY_DT) AS ASSOC_7_MEMBER_EXPIRY_DT,
ASSOC_7_FIRST_NAME,
ASSOC_7_MI,
ASSOC_7_LAST_NAME,
ASSOC_7_FULL_NAME,
ASSOC_7_LINE_ITEM_DES,
ASSOC_7_LINE_ITEM_AMT,
ASSOC_7_ALTERNATE_AMT,
TOTAL_ALTERNATE_AMT,
DONOR_FIRST_NAME,
DONOR_MI,
DONOR_LAST_NAME,
MEM_GROUP AS GROUP1, -- GROUP is Reserved word and we cannot name column name as GROUP. 
LINE_ITEM,
DESCRIPTION,
AMOUNT,
ADDITIONAL_DUES_AND_FEES,
TOTAL_AMT_DUE,
TOTAL_AMT_PAID,
FORMAT_DATE("%Y-%m-%d",PAYMENT_DUE_DATE) AS PAYMENT_DUE_DATE,
PLUS_UPGRADE_AMT,
PREMIER_UPGRADE_AMT,
ASSOCIATE_CHARGE_RATE,
BILL_PAY_METHOD_CD,
CURRENTLY_HAS_AUTO,
CURRENTLY_HAS_HOME,
ISSUING_STATE,
PREFERRED_LANGUAGE
FROM
  LANDING.STAGE_MEMBERSHIP_BILL
  WHERE EVENT_TYPE='BILL2'
    AND cast(UPDATE_DTTIME as date)=CURRENT_DATE()
    --AND FILE_GENERATED='N'
    ;
