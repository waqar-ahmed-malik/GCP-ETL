SELECT 
cast (ROID as int64 )  as RO_ID ,
cast (ROSTATUS as string )  as RO_STATUS ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p', CREATEDATE)  as CREATE_DATE ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p',COMPLETEDATE )  as COMPLETE_DATE ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p',INVOICEDATE)  as INVOICE_DATE ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p',PROMISEDDATE  )  as PROMISED_DATE ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p',CANCELEDDATE)  as CANCELED_DATE ,
cast (CUSTID as int64 )  as CUST_ID ,
cast (VEHID as int64 )  as VEH_ID ,
cast (TAXCAT as int64 )  as TAXCAT ,
cast (SRVWNAME as string )  as SRVWNAME ,
cast (RONOTE as string )  as RO_NOTE ,
cast (CONTACTNAME as string )  as CONTACT_NAME ,
cast (CONTACTPHONE as string )  as CONTACT_PHONE ,
cast (TECH as string )  as TECH ,
cast (BAYNUM as string )  as BAYNUM ,
cast (DURATION as float64 )  as DURATION ,
PARSE_DATETIME('%m/%d/%Y %H:%M %p',SCHEDDATE )  as SCHED_DATE ,
cast (AUTHOR as string )  as AUTHOR ,
cast (AUTHORSTATION as string )  as AUTHORSTATION ,
cast (ADVERSRC as string )  as ADVERSRC ,
cast (ARACCT as string )  as ARACCT ,
cast (SHOPSUPPLIESACCT as string )  as SHOPSUPPLIESACCT ,
cast (SALESTAXACCT as string )  as SALESTAXACCT ,
cast (ROREF as string )  as ROREF,
cast (CUSTPO as string )  as CUSTPO ,
cast (LABORTOTAL as float64 )  as LABORTOTAL ,
cast (PARTTOTAL as float64 )  as PART_TOTAL ,
cast (SUBLETTOTAL as float64 )  as SUBLET_TOTAL ,
cast (CHARGETOTAL as float64 )  as CHARGE_TOTAL ,
cast (SUPPLYCRG as float64 )  as SUPPLYCRG ,
cast (LABORTAXABLE as float64 )  as LABORTAXABLE ,
cast (PARTTAXABLE as float64 )  as PARTTAXABLE ,
cast (SUBLETTAXABLE as float64 )  as SUBLETTAXABLE ,
cast (CHARGETAXABLE as float64 )  as CHARGETAXABLE ,
cast (SUPCRGTAXABLE as float64 )  as SUPCRGTAXABLE ,
cast (TOTALTAXABLE as float64 )  as TOTALTAXABLE ,
cast (SUBTOTAL as float64 )  as SUBTOTAL ,
cast (TOTAL as float64 )  as TOTAL ,
cast (TAX as float64 )  as TAX ,
cast (TOTALTAX as float64 )  as TOTALTAX ,
cast (CPID as int64 )  as CP_ID,
cast (CPNAME as string )  as CPNAME ,
cast (DISCOUNTLABORTOTAL as float64 )  as DISCOUNTLABORTOTAL ,
cast (DISCOUNTPARTTOTAL as float64 )  as DISCOUNTPARTTOTAL ,
cast (DISCOUNTSUBLETTOTAL as float64 )  as DISCOUNTSUBLETTOTAL ,
cast (DISCOUNTDISCOUNTTOTAL as float64 )  as DISCOUNTDISCOUNTTOTAL ,
cast (CUSTWAITING as string )  as CUSTWAITING ,
cast (RETURNPARTS as string )  as RETURNPARTS ,
cast (ORIGPRINTDATE as datetime )  as ORIGPRINTDATE ,
cast (ODOMETERIN as string )  as ODOMETERIN ,
cast (ODOMETEROUT as string )  as ODOMETEROUT ,
cast (SUPCRGOVERRIDE as string )  as SUPCRGOVERRIDE ,
cast (COMPLETEYN as string )  as COMPLETEYN ,
cast (CLASSNAME as string )  as CLASS_NAME ,
cast (ESTPRINTDEALERHEADERYN as string )  as ESTPRINTDEALERHEADERYN ,
cast (ROPRINTDEALERHEADERYN as string )  as ROPRINTDEALERHEADERYN ,
cast (JOBPRINTDEALERHEADERYN as string )  as JOBPRINTDEALERHEADERYN ,
cast (INVPRINTDEALERHEADERYN as string )  as INVPRINTDEALERHEADERYN ,
cast (ESTPRINTMESSAGEYN as string )  as ESTPRINTMESSAGEYN ,
cast (ROPRINTMESSAGEYN as string )  as ROPRINTMESSAGEYN ,
cast (JOBPRINTMESSAGEYN as string )  as JOBPRINTMESSAGEYN ,
cast (INVPRINTMESSAGEYN as string )  as INVPRINTMESSAGEYN ,
cast (ESTPRINTHOURSYN as string )  as ESTPRINTHOURSYN ,
cast (ROPRINTHOURSYN as string )  as ROPRINTHOURSYN ,
cast (JOBPRINTHOURSYN as string )  as JOBPRINTHOURSYN ,
cast (INVPRINTHOURSYN as string )  as INVPRINTHOURSYN ,
cast (ESTPRINTITEMYN as string )  as ESTPRINTITEMYN ,
cast (ROPRINTITEMYN as string )  as ROPRINTITEMYN ,
cast (JOBPRINTITEMYN as string )  as JOBPRINTITEMYN ,
cast (INVPRINTITEMYN as string )  as INVPRINTITEMYN ,
cast (ESTPRINTTIMEYN as string )  as ESTPRINTTIMEYN ,
cast (ROPRINTTIMEYN as string )  as ROPRINTTIMEYN ,
cast (JOBPRINTTIMEYN as string )  as JOBPRINTTIMEYN ,
cast (INVPRINTTIMEYN as string )  as INVPRINTTIMEYN ,
cast (ROPRINTAPPROVALSYN as string )  as ROPRINTAPPROVALSYN ,
cast (INVPRINTAPPROVALSYN as string )  as INVPRINTAPPROVALSYN ,
cast (RONUM as int64 )  as RONUM ,
cast (ESTPRINTMAILINGYN as string )  as ESTPRINTMAILINGYN ,
cast (ROPRINTMAILINGYN as string )  as ROPRINTMAILINGYN ,
cast (INVPRINTMAILINGYN as string )  as INVPRINTMAILINGYN ,
cast (ESTPRINTDISCOUNTYN as string )  as ESTPRINTDISCOUNTYN ,
cast (ROPRINTDISCOUNTYN as string )  as ROPRINTDISCOUNTYN ,
cast (INVPRINTDISCOUNTYN as string )  as INVPRINTDISCOUNTYN ,
cast (FIXTYPE as string )  as FIXTYPE ,
cast (TAXRATE as string )  as TAXRATE ,
cast (NEGROID as int64 )  as NEGROID ,
cast (ESTGROUPYN as string )  as ESTGROUPYN ,
cast (INVGROUPYN as string )  as INVGROUPYN ,
cast (ROGROUPYN as string )  as ROGROUPYN ,
cast (EST_DISC_ID as int64 )  as EST_DISC_ID ,
cast (RO_DISC_ID as int64 )  as RO_DISC_ID ,
cast (INV_DISC_ID as int64 )  as INV_DISC_ID ,
cast (PAIDYN as string )  as PAIDYN ,
cast (PAIDNOTE as string )  as PAIDNOTE ,
cast (LABORTAX as float64 )  as LABORTAX ,
cast (PARTTAX as float64 )  as PARTTAX ,
cast (SUBLETTAX as float64 )  as SUBLETTAX ,
cast (CHARGETAX as float64 )  as CHARGETAX ,
cast (SUPCRGTAX as float64 )  as SUPCRGTAX ,
cast (READYEMAILSENTYN as string )  as READYEMAILSENTYN ,
cast (PROFITCENTER as string )  as PROFITCENTER ,
cast (ARRIVEDBY as string )  as ARRIVEDBY ,
cast (PRIMARYFAILURE as string )  as PRIMARYFAILURE ,
cast (SECONDARYFAILURE as string )  as SECONDARYFAILURE ,
cast (JOBPRINTPRICESYN as string )  as JOBPRINTPRICESYN ,
cast (JOBPRINTNOTESYN as string )  as JOBPRINTNOTESYN ,
cast (SHOWACTHRSONSUMSCREENYN as string )  as SHOWACTHRSONSUMSCREENYN ,
cast (JOBPRINTCUSTINFOYN as string )  as JOBPRINTCUSTINFOYN 
FROM COR_ANALYTICS.STG_NAPATRACKS_ROHSUB7