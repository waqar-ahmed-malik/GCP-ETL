--Overall flow of these script is
--Step0 Prepare the IVANS_IE Customer data for the MDM process
-- #Step1 update dimension records that have already been tied to records in the bridge table
-- #Step2 Insert records into the bridge table for records that we have a match on the MEMBER_ID
-- #Step3 Insert records into the bridge table for records that we have a match on the name, and address
-- #Step4 Insert records into the bridge table for records that we have a match on the name, and email address
-- #Step5 Insert new records into the dimension table and bridge table for any new members
-- #MDM Process starts by creating the initial work table that contains all customer data currently in the system with fields standardized for MDM

--First step is to update records in the mdm_customer_dim that have previously been linked and then delete those records from the working table

--Deleting duplicates from bridge table
DELETE FROM CUSTOMER_PRODUCT.MDM_CUSTOMER_BRIDGE C WHERE 1=2
