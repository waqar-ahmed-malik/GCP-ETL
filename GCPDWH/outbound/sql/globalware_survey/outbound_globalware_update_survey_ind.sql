update OPERATIONAL.GLOBALWARE_INVOICE SET SURVEY_IND='Y'
where PNR_LOCATOR	in (select PnrLocator from LANDING.OUTBOUND_GLOBALWARE_SURVEY)
AND SURVEY_IND<>'Y'
