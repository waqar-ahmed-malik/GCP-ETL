SELECT COUNT(*) FROM CUSTOMERS.MEMBERSHIP_CUSTOMER_DIM WHERE ROW_START_DT =  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)