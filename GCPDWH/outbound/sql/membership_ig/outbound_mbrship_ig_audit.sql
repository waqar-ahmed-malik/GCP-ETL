select CONCAT(IG_STRING,'\n',IG_STRING_MKT) from
(SELECT DISTINCT '1' as JOIN_COND,
concat('Total_number_of_Active_Memberships_in_Membership_Platform=0','\n',
'Total_number_of_Active_Members_in_Membership_Platform=0','\n',
'Total_number_of_Inactive_Members_in_Membership_Platform=0','\n',
'Total_number_of_Pending_Members_in_Membership_Platform=0','\n',
'Total_number_of_new_Active_Memberships_in_Membership_Platform_created_on_each_day=0','\n',
'Total_number_of_new_Active_Members_in_Membership_Platform_created_on_each_day=0','\n',
'Total_Record_Count_CPMS_FEED=',CAST(count(*) AS STRING)) AS IG_STRING
FROM LANDING.OUTBOUND_STAGE_MBRS_IG) IG,
(SELECT DISTINCT '1' as JOIN_COND,concat('Total_Record_Count_MARKETING_FEED=',CAST(count(*) AS STRING),'\n',
'Total_number_of_Transferin_Members_in_Membership_Platform=0') AS IG_STRING_MKT
FROM LANDING.OUTBOUND_STAGE_IG_MARKETING) IG_MKT
WHERE IG.JOIN_COND=IG_MKT.JOIN_COND