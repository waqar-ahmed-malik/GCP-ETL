-- One time script to copy existing employee_dim to landing dataset
CREATE OR REPLACE TABLE LANDING.EMPLOYEE_DIM_STAGE AS
(SELECT * FROM CUSTOMER_PRODUCT.EMPLOYEE_DIM)