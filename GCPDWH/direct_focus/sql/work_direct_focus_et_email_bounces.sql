SELECT
 LTRIM(RTRIM(CONVERT(CHAR,ClientID,120))) AS CLIENT_ID,
 LTRIM(RTRIM(CONVERT(CHAR,SendID,120))) AS SEND_ID,
 LTRIM(RTRIM(CONVERT(CHAR,SubscriberKey,120))) AS SUBSCRIBER_KEY,
 LTRIM(RTRIM(CONVERT(CHAR,EmailAddress,120))) AS EMAIL_ADDRESS,
 LTRIM(RTRIM(CONVERT(CHAR,SubscriberID,120))) AS SUBSCRIBER_ID,
 LTRIM(RTRIM(CONVERT(CHAR,ListID,120))) AS LIST_ID,
 LTRIM(RTRIM(CONVERT(CHAR,EventDate,120))) AS EVENT_DTTIME,
 LTRIM(RTRIM(CONVERT(CHAR,EventType,120))) AS EVENT_TYPE,
 LTRIM(RTRIM(CONVERT(CHAR,BounceCategory,120))) AS BOUNCE_CATEGORY,
 LTRIM(RTRIM(CONVERT(CHAR,SMTPCode,120))) AS SMTP_CD,
 LTRIM(RTRIM(CONVERT(CHAR,BounceReason,120))) AS BOUNCE_REASON,
 LTRIM(RTRIM(CONVERT(CHAR,BatchID,120))) AS BATCH_ID,
 LTRIM(RTRIM(CONVERT(CHAR,TriggeredSendExternalKey,120))) AS TRIGGERED_SEND_EXTERNAL_KEY,
 LTRIM(RTRIM(CONVERT(CHAR,RECNO,120))) AS REC_NUM,
 LTRIM(RTRIM(CONVERT(CHAR,ARCH_DATE,120))) AS ARCH_DTTIME
FROM DF_NCNU_CLUB.dbo.ET_EmailBounces
WHERE CONVERT(date,ARCH_DATE) >= DATEADD(day,-v_incr_date,convert(date, 'v_inputdate'))
AND CONVERT(date,ARCH_DATE) < CONVERT(date,'v_inputdate')
--WHERE LTRIM(RTRIM(CONVERT(date,ARCH_DATE)))>'v_incr_date'