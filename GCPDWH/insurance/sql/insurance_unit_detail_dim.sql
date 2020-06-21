CREATE OR REPLACE TABLE  CUSTOMER_PRODUCT.INSURANCE_UNIT_DETAILS_DIM AS  (

SELECT
 INSURANCE_UNIT_ID
,UNIT_SEQUENCE_ID
,SOURCE_ID
,DATA_SOURCE
,SOURCE_PRODUCT_CD
,PRODUCT_CD
,PRODUCT_TYPE
,SUB_PRODUCT_TYPE
,POLICY_NUM
,AGREEMENT_NUM
,UNIT_TYPE_CD
,UNIT_TYPE_DESC
,RISK_STATE_CD
,RATED_TERRITORY_CD
,RATED_TERRITORY_DESC
,UNIT_YEAR
,AUTO_BODILY_INJURY_PERSON_AMT	
,AUTO_BODILY_INJURY_OCCURRENCE_AMT 
,AUTO_PROPERTY_DAMAGE_AMT		
,AUTO_UNINSURED_MOTORISTS_PERSON_AMT		
,AUTO_UNINSURED_MOTORISTS_OCCURRENCE_AMT		
,AUTO_MEDICAL_PAYMENT_AMT		
,AUTO_COLLISION_DEDUCTIBLE		
,AUTO_COMPREHENSIVE_DEDUCTIBLE		
,AUTO_RENTAL_PERSON_AMT		
,AUTO_RENTAL_OCCURRENCE_AMT		
,AUTO_GLASS
,AUTO_ANNUAL_MILEAGE_TO_WORK
,AUTO_ANNUAL_MILEAGE
,AUTO_MULTI_VEHICLE_DISCOUNT_IND
,AUTO_WORK_DAYS_CNT
,AUTO_VIN
,AUTO_VEHICLE_MODEL_NM 
,AUTO_VEHICLE_MAKE_NM 
,AUTO_VEHICLE_BODY_TYPE_CD
,AUTO_VEHICLE_ODOMETER_READING
,AUTO_VEHICLE_STATED_AMT
,AUTO_RATED_CLASS_CD
,AUTO_PERFORMANCE_CD
,AUTO_RATED_SYMBOL_CD
,AUTO_RATED_SYMBOL_DESC
,AUTO_USE_CD
,AUTO_USE_DESC
,HOME_POLICY_DWELLING_AMT
,HOME_POLICY_PROPERTY_AMT
,HOME_OTHER_STRUCTER_AMT
,HOME_LOSS_OF_USE_AMT
,HOME_PERSONAL_LIABILITY_EACH_OCCURRENCE
,HOME_MEDICAL_PAYMENT_TO_OTHERS
,HOME_DEDUCTIBLE
,HOME_REPLACEMENT_AMT
,HOME_BURGLAR_ALARM_TYPE_CD
,HOME_BURGLAR_ALARM_TYPE_DESC	
,HOME_CONSTRUCTION_TYPE_CD		
,HOME_CONSTRUCTION_TYPE_DESC		
,HOME_ESTIMATED_HEAD_CNT
,HOME_FAMILY_CNT
,HOME_FIRE_ALARM_TYPE_CD
,HOME_FIRE_ALARM_TYPE_DESC
,HOME_TYPE_CD
,HOME_TYPE_DESC
,HOME_MARKET_VALUE_AMT
,HOME_OCCUPANCY_TYPE_CD
,HOME_OCCUPANCY_TYPE_DESC
,HOME_OCCUPANT_CNT
,HOME_PROTECTION_CLASS_CD
,HOME_PROTECTION_CLASS_DESC
,HOME_ROOF_BUILT_YEAR
,HOME_ROOF_TYPE_CD
,HOME_ROOF_TYPE_DESC
,HOME_ROOM_CNT
,HOME_SMOKE_ALARM_IND
,HOME_SMOKE_DETECTOR_IND
,HOME_SQUARE_FEET_CNT
,HOME_SWIMMING_POOL_TYPE_CD
,HOME_SWIMMING_POOL_TYPE_DESC
,PERSONAL_PROPERTY_FLAG
,PERSONAL_PROPERTY_QUANTITY
,PERSONAL_PROPERTY_AMT
,TRAILER_MAKE_NM
,TRAILER_MODEL_NM
,TRAILER_AMT
,HORSEPOWER_CNT
,JOB_RUN_ID
,SOURCE_SYSTEM_CD
,CREATE_DTTIME
FROM (

SELECT 
  
CONCAT(POLICY_NUM , CAST(UNIT_SEQUENCE_ID AS STRING) ) AS INSURANCE_UNIT_ID,
UNIT_SEQUENCE_ID,
SOURCE_ID,
DATA_SOURCE,
SOURCE_PRODUCT_CD,
PRODUCT_CD,
PRODUCT_TYPE,
SUB_PRODUCT_TYPE,
POLICY_NUM,
AGREEMENT_NUM,
UNIT_TYPE_CD, 
UNIT_TYPE_DESC,
RISK_STATE_CD,
RATED_TERRITORY_CD,
RATED_TERRITORY_DESC,
UNIT_YEAR, 
AUTO_BODILY_INJURY_PERSON_AMT		,
AUTO_BODILY_INJURY_OCCURRENCE_AMT ,
AUTO_PROPERTY_DAMAGE_AMT		,
AUTO_UNINSURED_MOTORISTS_PERSON_AMT		,
AUTO_UNINSURED_MOTORISTS_OCCURRENCE_AMT		,
AUTO_MEDICAL_PAYMENT_AMT		,
AUTO_COLLISION_DEDUCTIBLE		,
AUTO_COMPREHENSIVE_DEDUCTIBLE		,
AUTO_RENTAL_PERSON_AMT		,
AUTO_RENTAL_OCCURRENCE_AMT		,
AUTO_GLASS, 
AUTO_ANNUAL_MILEAGE_TO_WORK,
AUTO_ANNUAL_MILEAGE,
AUTO_MULTI_VEHICLE_DISCOUNT_IND,
AUTO_WORK_DAYS_CNT,
AUTO_VIN,
AUTO_VEHICLE_MODEL_NM,
AUTO_VEHICLE_MAKE_NM,
AUTO_VEHICLE_BODY_TYPE_CD,
AUTO_VEHICLE_ODOMETER_READING,
AUTO_VEHICLE_STATED_AMT,
AUTO_RATED_CLASS_CD,
AUTO_PERFORMANCE_CD,
AUTO_RATED_SYMBOL_CD,
AUTO_RATED_SYMBOL_DESC,
AUTO_USE_CD,
AUTO_USE_DESC,
HOME_POLICY_DWELLING_AMT,
HOME_POLICY_PROPERTY_AMT,
HOME_OTHER_STRUCTER_AMT,
HOME_LOSS_OF_USE_AMT,
HOME_PERSONAL_LIABILITY_EACH_OCCURRENCE,
HOME_MEDICAL_PAYMENT_TO_OTHERS,
HOME_DEDUCTIBLE,
HOME_REPLACEMENT_AMT,
HOME_BURGLAR_ALARM_TYPE_CD,
HOME_BURGLAR_ALARM_TYPE_DESC,
HOME_CONSTRUCTION_TYPE_CD,
HOME_CONSTRUCTION_TYPE_DESC,
HOME_ESTIMATED_HEAD_CNT,
HOME_FAMILY_CNT,
HOME_FIRE_ALARM_TYPE_CD,
HOME_FIRE_ALARM_TYPE_DESC,
HOME_TYPE_CD,
HOME_TYPE_DESC,
HOME_MARKET_VALUE_AMT,
HOME_OCCUPANCY_TYPE_CD,
HOME_OCCUPANCY_TYPE_DESC,
HOME_OCCUPANT_CNT,
HOME_PROTECTION_CLASS_CD,
HOME_PROTECTION_CLASS_DESC,
HOME_ROOF_BUILT_YEAR,
HOME_ROOF_TYPE_CD,
HOME_ROOF_TYPE_DESC,
HOME_ROOM_CNT,
HOME_SMOKE_ALARM_IND,
HOME_SMOKE_DETECTOR_IND,
HOME_SQUARE_FEET_CNT,
HOME_SWIMMING_POOL_TYPE_CD,
HOME_SWIMMING_POOL_TYPE_DESC,
PERSONAL_PROPERTY_FLAG,
PERSONAL_PROPERTY_QUANTITY,
PERSONAL_PROPERTY_AMT,
TRAILER_MAKE_NM,
TRAILER_MODEL_NM,
TRAILER_AMT,
HORSEPOWER_CNT,
'V_JOB_RUN_ID'  AS JOB_RUN_ID,
'INSURANCE' AS SOURCE_SYSTEM_CD,
DATETIME(CURRENT_TIMESTAMP()) AS CREATE_DTTIME
FROM (

SELECT DISTINCT
CONCAT("IE~",CAST(C1.FILE_ID AS STRING),"~",CAST(C1.TRANS_NUM AS STRING)) AS SOURCE_ID,
H1.UNT_NUM AS UNIT_SEQUENCE_ID,
A1.DATA_SOURCE_FILE AS DATA_SOURCE,
TRIM(P1.PRODUCT_CD) AS SOURCE_PRODUCT_CD,
TRIM(P1.PRODUCT_CD) AS PRODUCT_CD,
C1.PLCY_NUM POLICY_NUM,
CASE WHEN C1.PLCY_PRFX IS NOT NULL THEN  CONCAT(C1.PLCY_PRFX,C1.PLCY_NUM)
	ELSE CONCAT(H1.RT_TERR_ST_CD, P1.PRODUCT_CD,C1.PLCY_NUM) END AS AGREEMENT_NUM,
P1.PRODUCT_TYPE,
P1.SUB_PRODUCT_TYPE,
TRIM(H1.UNT_TYP_CD) AS UNIT_TYPE_CD,
CLUT.CODE_DESC AS UNIT_TYPE_DESC,
TRIM(H1.RT_TERR_ST_CD) AS RISK_STATE_CD,
TRIM(H1.RT_TERR_CD) AS RATED_TERRITORY_CD,
CLRT.CODE_DESC AS RATED_TERRITORY_DESC,
I1.UNT_YR AS UNIT_YEAR,
I1.MI_WRK_CNT AS AUTO_ANNUAL_MILEAGE_TO_WORK,
I1.ANNL_MI_NUM AS AUTO_ANNUAL_MILEAGE,
TRIM(I1.MUL_VH_DISC_IND) AS AUTO_MULTI_VEHICLE_DISCOUNT_IND,
I1.WRK_DYS_NUM AS AUTO_WORK_DAYS_CNT,
I1.VIN AS AUTO_VIN,
TRIM(I1.VEH_BDY_TYP_CD) AS AUTO_VEHICLE_BODY_TYPE_CD,
I1.VEH_ODMT_READ AS  AUTO_VEHICLE_ODOMETER_READING,
I1.VEH_STED_AMT AS AUTO_VEHICLE_STATED_AMT,
TRIM(I1.RT_CLAS_CD) AS AUTO_RATED_CLASS_CD,
TRIM(I1.PERF_CD) AS AUTO_PERFORMANCE_CD,
TRIM(I1.RT_SYMBOL_CD) AS AUTO_RATED_SYMBOL_CD,
CLRS.CODE_DESC AS AUTO_RATED_SYMBOL_DESC,
TRIM(I1.USE_CD) AS AUTO_USE_CD,
CLU.CODE_DESC    AS AUTO_USE_DESC,
I1.MDEL_NM AS AUTO_VEHICLE_MODEL_NM,
I1.MK_NM AS AUTO_VEHICLE_MAKE_NM,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_A.COVRG_AMT 
	ELSE NULL END  AS HOME_POLICY_DWELLING_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_C.COVRG_AMT 
	ELSE NULL END  AS HOME_POLICY_PROPERTY_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_BI.COVRG_LIM_PERS_AMT 
	ELSE NULL END  AS AUTO_BODILY_INJURY_PERSON_AMT,
	CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_BI.COVRG_LIM_OCC_AMT 
	ELSE NULL END  AS AUTO_BODILY_INJURY_OCCURRENCE_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_PD.COVRG_AMT 
	ELSE NULL END   AS AUTO_PROPERTY_DAMAGE_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_UN.COVRG_LIM_PERS_AMT
	ELSE NULL END   AS AUTO_UNINSURED_MOTORISTS_PERSON_AMT,
	CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_UN.COVRG_LIM_OCC_AMT
	ELSE NULL END   AS AUTO_UNINSURED_MOTORISTS_OCCURRENCE_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_MD.COVRG_AMT 
	ELSE NULL END   AS AUTO_MEDICAL_PAYMENT_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_CD.DED_AMT 
	ELSE NULL  END   AS AUTO_COLLISION_DEDUCTIBLE,
CASE WHEN P1.PRODUCT_TYPE ='Auto' THEN K1_CDD.DED_AMT
	ELSE NULL END   AS AUTO_COMPREHENSIVE_DEDUCTIBLE,
CASE WHEN  P1.PRODUCT_TYPE ='Auto' THEN K1_AR.COVRG_LIM_PERS_AMT
	ELSE NULL END  AS AUTO_RENTAL_PERSON_AMT,
CASE WHEN  P1.PRODUCT_TYPE ='Auto' THEN K1_AR.COVRG_LIM_OCC_AMT
	ELSE NULL END  AS AUTO_RENTAL_OCCURRENCE_AMT,
K1_AG.COVRG_AMT AS AUTO_GLASS,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_HS.COVRG_AMT 
	ELSE NULL END AS HOME_OTHER_STRUCTER_AMT,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_HL.COVRG_AMT 
	ELSE NULL END AS HOME_LOSS_OF_USE_AMT,
CASE WHEN  P1.PRODUCT_TYPE ='Home' THEN K1_HO.COVRG_AMT  
	ELSE NULL END  AS HOME_PERSONAL_LIABILITY_EACH_OCCURRENCE,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_HM.COVRG_AMT 
	ELSE NULL END  AS HOME_MEDICAL_PAYMENT_TO_OTHERS,
CASE WHEN P1.PRODUCT_TYPE ='Home' THEN K1_HM.DED_AMT 
	ELSE NULL END  AS HOME_DEDUCTIBLE,
I2.REPL_AMT AS HOME_REPLACEMENT_AMT ,
I3.TRLR1_MK_NM  AS TRAILER_MAKE_NM,
I3.TRLR1_MDEL_NM AS TRAILER_MODEL_NM,
I3.TRLR1_AMT AS TRAILER_AMT,
I3.HPW_NUM AS HORSEPOWER_CNT,
TRIM(I2.BURG_ALRM_TYP_CD) AS  HOME_BURGLAR_ALARM_TYPE_CD,
CLBA.CODE_DESC  AS HOME_BURGLAR_ALARM_TYPE_DESC,
TRIM(I2.CONST_TYP_CD) AS  HOME_CONSTRUCTION_TYPE_CD,
CLCT.CODE_DESC AS  HOME_CONSTRUCTION_TYPE_DESC,
I2.EST_HEAD_CNT AS  HOME_ESTIMATED_HEAD_CNT,
I2.FMLY_NUM AS  HOME_FAMILY_CNT,
TRIM(I2.FIR_ALRM_TYP_CD) AS  HOME_FIRE_ALARM_TYPE_CD,
CLFA.CODE_DESC AS  HOME_FIRE_ALARM_TYPE_DESC,
TRIM(I2.HO_TYP_CD) AS  HOME_TYPE_CD,
CLHT.CODE_DESC AS  HOME_TYPE_DESC,
I2.MKT_VAL_AMT AS  HOME_MARKET_VALUE_AMT,
TRIM(I2.OCPY_TYP_CD) AS  HOME_OCCUPANCY_TYPE_CD,
CLOT.CODE_DESC AS  HOME_OCCUPANCY_TYPE_DESC,
I2.OCPN_NUM AS  HOME_OCCUPANT_CNT,
TRIM(I2.PRTC_CLAS_CD) AS  HOME_PROTECTION_CLASS_CD,
CLPC.CODE_DESC AS  HOME_PROTECTION_CLASS_DESC,
I2.RF_BLT_YR AS  HOME_ROOF_BUILT_YEAR,
TRIM(I2.RF_TYP_CD) AS  HOME_ROOF_TYPE_CD,
CLHR.CODE_DESC AS  HOME_ROOF_TYPE_DESC,
I2.RM_CNT AS  HOME_ROOM_CNT,
TRIM(I2.SMK_ALRM_IND) AS  HOME_SMOKE_ALARM_IND,
TRIM(I2.SMK_DTCT_IND) AS  HOME_SMOKE_DETECTOR_IND,
I2.SQ_FT_NUM AS  HOME_SQUARE_FEET_CNT,
TRIM(I2.SWMGPL_TYP_CD) AS  HOME_SWIMMING_POOL_TYPE_CD,
CLSP.CODE_DESC AS  HOME_SWIMMING_POOL_TYPE_DESC,
CASE WHEN M1.TRANS_NUM IS NOT NULL THEN 'Y' ELSE 'N' END AS PERSONAL_PROPERTY_FLAG,
M1.PERSONAL_PROPERTY_QUANTITY AS PERSONAL_PROPERTY_QUANTITY,
M1.PERSONAL_PROPERTY_AMT AS PERSONAL_PROPERTY_AMT
FROM  (SELECT * FROM 
(SELECT *, ROW_NUMBER() OVER (PARTITION BY  PLCY_NUM  ORDER BY FILE_ID DESC , TRANS_NUM DESC) RN FROM `LANDING.IE_C1_LDG` ) C1
WHERE C1.RN=1 ) C1
INNER JOIN `LANDING.IE_A1_LDG` A1 ON A1.TRANS_NUM =C1.TRANS_NUM and A1.FILE_ID = C1.FILE_ID AND DATA_SOURCE_FILE = 'EXIGEN'
LEFT OUTER JOIN `LANDING.IE_H1_LDG`H1 ON C1.TRANS_NUM =H1.TRANS_NUM and C1.FILE_ID = H1.FILE_ID
LEFT OUTER JOIN `LANDING.IE_I1_LDG` I1 ON H1.TRANS_NUM =I1.TRANS_NUM and H1.FILE_ID = I1.FILE_ID and H1.UNT_NUM = I1.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_I2_LDG` I2 ON H1.TRANS_NUM =I2.TRANS_NUM and H1.FILE_ID = I2.FILE_ID and H1.UNT_NUM = I2.ITEM_NUM
LEFT OUTER  JOIN `LANDING.IE_I3_LDG`I3 ON H1.TRANS_NUM =I3.TRANS_NUM and H1.FILE_ID = I3.FILE_ID and H1.UNT_NUM = I2.ITEM_NUM
LEFT OUTER JOIN `REFERENCE.SALES_PRODUCT_DIM` P1 ON SUBSTR(C1.PLCY_PRFX,3,2) = P1.PRODUCT_CD AND P1.SOURCE_SYSTEM_CD='IVANS'
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_A ON K1_A.COVRG_CD IN ('A', 'CovA' ) AND H1.TRANS_NUM=K1_A.TRANS_NUM and H1.FILE_ID = K1_A.FILE_ID and H1.UNT_NUM = K1_A.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_C ON K1_C.COVRG_CD IN ('C', 'CovC' ) AND H1.TRANS_NUM=K1_C.TRANS_NUM and H1.FILE_ID = K1_C.FILE_ID  and H1.UNT_NUM = K1_C.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_BI ON K1_BI.COVRG_CD IN ('BI','42') AND H1.TRANS_NUM=K1_BI.TRANS_NUM and H1.FILE_ID = K1_BI.FILE_ID  and H1.UNT_NUM = K1_BI.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_PD ON K1_PD.COVRG_CD IN ('PD' , '2' ) AND H1.TRANS_NUM=K1_PD.TRANS_NUM and H1.FILE_ID = K1_PD.FILE_ID  and H1.UNT_NUM = K1_PD.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_UN  ON K1_UN.COVRG_CD IN ('UMPDDED','UMPDDED','UMBI','UMPD','UMISP','UMCSL','9') AND H1.TRANS_NUM=K1_UN.TRANS_NUM and H1.FILE_ID = K1_UN.FILE_ID and H1.UNT_NUM = K1_UN.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_MD ON K1_MD.COVRG_CD IN ('PIP','MEDPM','7') AND H1.TRANS_NUM=K1_MD.TRANS_NUM and H1.FILE_ID = K1_MD.FILE_ID and H1.UNT_NUM = K1_MD.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_CD ON K1_CD.COVRG_CD IN ('COLLDED','33')  AND H1.TRANS_NUM=K1_CD.TRANS_NUM and H1.FILE_ID = K1_CD.FILE_ID and H1.UNT_NUM = K1_CD.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_CDD ON K1_CDD.COVRG_CD IN ('COMPDED','44') AND H1.TRANS_NUM=K1_CDD.TRANS_NUM and H1.FILE_ID = K1_CDD.FILE_ID and H1.UNT_NUM = K1_CDD.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_AR ON K1_AR.COVRG_CD IN ('ETEC','26') AND H1.TRANS_NUM=K1_AR.TRANS_NUM and H1.FILE_ID = K1_AR.FILE_ID and H1.UNT_NUM = K1_AR.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_AG ON K1_AG.COVRG_CD IN ('GLASS','45') AND H1.TRANS_NUM=K1_AG.TRANS_NUM and H1.FILE_ID = K1_AG.FILE_ID and H1.UNT_NUM = K1_AG.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_HS ON K1_HS.COVRG_CD IN ('CovB') AND H1.TRANS_NUM=K1_HS.TRANS_NUM and H1.FILE_ID = K1_HS.FILE_ID and H1.UNT_NUM = K1_HS.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_HL ON K1_HL.COVRG_CD IN ('CovD') AND H1.TRANS_NUM=K1_HL.TRANS_NUM and H1.FILE_ID = K1_HL.FILE_ID and H1.UNT_NUM = K1_HL.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_HO ON K1_HO.COVRG_CD IN ('CovE') AND H1.TRANS_NUM=K1_HO.TRANS_NUM and H1.FILE_ID = K1_HO.FILE_ID and H1.UNT_NUM = K1_HO.ITEM_NUM
LEFT OUTER JOIN `LANDING.IE_K1_LDG` K1_HM ON K1_HM.COVRG_CD IN ('CovF') AND H1.TRANS_NUM=K1_HM.TRANS_NUM and H1.FILE_ID = K1_HM.FILE_ID and H1.UNT_NUM = K1_HM.ITEM_NUM
LEFT OUTER JOIN (SELECT SUM(PROP_VALUE) AS PERSONAL_PROPERTY_AMT, COUNT(TRANS_NUM) AS PERSONAL_PROPERTY_QUANTITY, TRANS_NUM, FILE_ID FROM `LANDING.IE_M1_LDG` GROUP BY TRANS_NUM,FILE_ID) M1 ON C1.TRANS_NUM = M1.TRANS_NUM and C1.FILE_ID = M1.FILE_ID
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLUT ON CLUT.CODE_VALUE = H1.UNT_TYP_CD AND CLUT.COLUMN_NAME = 'UNIT_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLRT ON CLRT.CODE_VALUE = H1.RT_TERR_CD AND CLRT.COLUMN_NAME = 'RATED_TERRITORY_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLRS ON CLRS.CODE_VALUE = I1.RT_SYMBOL_CD AND CLRS.COLUMN_NAME = 'AUTO_RATED_SYMBOL_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLU ON CLU.CODE_VALUE = I1.USE_CD AND CLU.COLUMN_NAME = 'AUTO_USE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLBA ON CLBA.CODE_VALUE = I2.BURG_ALRM_TYP_CD AND CLBA.COLUMN_NAME = 'HOME_BURGLAR_ALARM_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLCT ON CLCT.CODE_VALUE = I2.CONST_TYP_CD AND CLCT.COLUMN_NAME = 'HOME_CONSTRUCTION_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLFA ON CLFA.CODE_VALUE = I2.FIR_ALRM_TYP_CD AND CLFA.COLUMN_NAME = 'HOME_FIRE_ALARM_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLHT ON CLHT.CODE_VALUE = I2.HO_TYP_CD AND CLHT.COLUMN_NAME = 'HOME_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLOT ON CLOT.CODE_VALUE = I2.OCPY_TYP_CD AND CLOT.COLUMN_NAME = 'HOME_OCCUPANCY_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLPC ON CLPC.CODE_VALUE = I2.PRTC_CLAS_CD AND CLPC.COLUMN_NAME = 'HOME_PROTECTION_CLASS_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLHR ON CLHR.CODE_VALUE = I2.RF_TYP_CD AND CLHR.COLUMN_NAME = 'HOME_ROOF_TYPE_CD'
LEFT OUTER JOIN `REFERENCE.INSURANCE_CODE_LOOKUP` CLSP ON CLSP.CODE_VALUE = I2.SWMGPL_TYP_CD AND CLSP.COLUMN_NAME = 'HOME_SWIMMING_POOL_TYPE_CD'
) 
))