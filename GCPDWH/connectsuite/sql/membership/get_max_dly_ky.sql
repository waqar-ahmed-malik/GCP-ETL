select max(safe_cast(SOURCE_TRANSACTION_ID AS INT64)) FROM CUSTOMER_PRODUCT.CONNECTSUITE_MEMBER_TRANSACTIONS_FACT WHERE SOURCE_SYSTEM_CD <> "ARIA_SFDC"