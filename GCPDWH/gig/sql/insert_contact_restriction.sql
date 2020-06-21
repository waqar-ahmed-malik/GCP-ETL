INSERT INTO 
CUSTOMER_PRODUCT.GIG_CONTACT_RESTRICTION
(CUST_EMAIL ,
RESTRICTION_TYPE ,
CREATE_DTTIME ,
CREATE_USER )
(select Email_Address,
"Unsubscribe",
CURRENT_DATETIME(),
"v_job_name"
from LANDING.GIG_UNSUBSCRIBE
where Email_Address not in (select cust_email from LANDING.gig_contact_restrictions where restriction_type='Unsubscribe'));