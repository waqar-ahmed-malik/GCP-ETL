SELECT
  mbr.mbr_ky,
  mbr.mbrs_id,
  NULL AS CUSTOMER_KEY,
  mbr.mbr_assoc_id,
  CONVERT(varchar(30), mbr.mbr_jn_aaa_dt, 120) AS mbr_jn_aaa_dt,
  CONVERT(varchar(30), mbr_jn_clb_dt, 120) AS mbr_jn_clb_dt,
  CONVERT(varchar(30), mbr_brth_dt, 120) AS mbr_brth_dt,
  mbr.mbr_dup_crd_ct,	
  mbr.mbr_mid_init_nm,
  mbr.mbr_lst_nm,
  mbr.mbr_fst_nm,
  mbr.mbr_sts_dsc_cd,
  mbr.mbr_sts_cd,
  mbr.mbr_sltn_cd,
  mbr.mbr_relat_cd,
  mbr.mbr_rsn_join_cd,
  mbr.mbr_prev_club_cd,
  mbr.mbr_prev_mbrs_id,
  mbr.mbr_do_not_ren_in,
  mbr.mbr_slct_cd,
  mbr.mbr_src_sls_cd,
  CONVERT(varchar(30), mbr.mbr_canc_dt, 120) AS mbr_canc_dt,
  mbr.mbr_comm_cd,
  'null' AS COMPENSATE_EMPLOYEE_ID,
  mbr.mbrs_ky,
  mbr.mbr_typ_cd,
  mbr.mbr_paid_by_cd,
  mbr.mbr_free_assoc_in,
   CONVERT(varchar(30), mbr.mbr_dupcd_req_dt, 120) AS mbr_dupcd_req_dt,
  'null' AS SECONDARY_COMPENSATE_EMPLOYEE_ID,
  mbr.mbr_dup_stck_ct,
  CONVERT(varchar(30), mbr.mbr_dupstck_req_dt, 120) AS mbr_dupstck_req_dt,
  CONVERT(varchar(30), mbr.mbr_actv_dt, 120) AS mbr_actv_dt,
  mbr.mbr_bil_cat_cd,
  mbr.mbr_ers_usage_yr1,
  mbr.mbr_ers_usage_yr2,
  mbr.mbr_ers_usage_yr3,
  mbr.mbr_prev_sts_dsc_cd,
  'null' AS REINSTATEMENT_CARD_PRINT_INDICATOR,
  mbr.dup_card_reason,
  mbr.mbr_do_not_ren_rsn,
  mbr.mbr_card_name,
  mbr.mbr_language,
  CONVERT(varchar(30), mbr.card_reqt_skip_dt, 120) AS card_reqt_skip_dt,
  CONVERT(varchar(30), mbr.mbr_eff_dt, 120) AS mbr_eff_dt,
  mbr.mbr_prev_comm_cd,
  CONVERT(varchar(30), mbr.mbr_dnr_expir_dt, 120) AS mbr_dnr_expir_dt,
  mbr.mbr_mita_pending,
  CONVERT(varchar(30), mbr.mbr_add_dt, 120) AS mbr_add_dt,
  mbr.mbr_renew_ct,
  CONVERT(varchar(30), mbr.mbr_reinst_dt, 120) AS mbr_reinst_dt,
  mbr.card_reqt_skip_reason,
  CONVERT(varchar(30), mbr.mbr_expir_dt, 120) AS mbr_expir_dt,
  mbr.pref_meth_cnt AS PREF_METH_COUNT,
  R.ride_comp_cd AS MEMBERSHIP_LEVEL,
	ISNULL( CONVERT(varchar(30), mbr.mbr_reinst_dt, 120),
	CONVERT(varchar(30), mbr.mbr_jn_clb_dt, 120)) AS TENURE_DT,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'BM' THEN 'Y'
    ELSE 'N'
  END AS IS_BOARD_MEMBER,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'SPRINT' THEN sp.SPCL_HNDL_CMT_TX
  END AS SPRN_IND,
  NULL AS SPRINT_ENROLL_DT,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'HM' THEN 'Y'
    ELSE 'N'
  END AS HONORARY_MEMBER_IND,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'CM' THEN 'Y'
    ELSE 'N'
  END AS COURTESY_MEMBER_IND,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'UM' THEN 'Y'
    ELSE 'N'
  END AS UTAH_50_YEAR_IND,
  CASE
    WHEN sp.SPCL_HNDL_CD IN ('SPP', 'SPA') THEN 'Y'
    ELSE 'N'
  END AS SERVICE_PROVIDER_IND,
  'null' AS SALES_CHANNEL,
  DATEPART(YEAR,
    mbr.mbr_jn_aaa_dt) AS MEMBER_SINCE,
  CASE
    WHEN sp.SPCL_HNDL_CD= 'NVIA' THEN 'Y'
    ELSE 'N'
  END AS VIA_MAGAZINE_PREFERRED_IND,
  'null' AS CORPORATE_GROUP_TYPE,
  NULL AS ETL_JOB_RUN_ID,
  'null' AS SOURCE_SYSTEM_CD,
  NULL AS CREATE_DT,
  NULL AS UPDATE_DT,
  'null' AS CREATE_BY
FROM
  mbr
LEFT OUTER JOIN
  sales_agent sa
ON
  mbr.sls_agt_ky = sa.sagt_ky
LEFT OUTER JOIN
  sales_agent sec_sa
ON
  mbr.MBR_SEC_SAGT_KY = sec_sa.sagt_ky
LEFT OUTER JOIN (
  SELECT
    *
  FROM (
    SELECT
      *,
      DENSE_RANK() OVER(PARTITION BY MBRS_KY ORDER BY CASE WHEN ride_comp_cd= 'BS' THEN 1 WHEN ride_comp_cd= 'BS' THEN 1 WHEN ride_comp_cd= 'PL' THEN 2 WHEN ride_comp_cd= 'RP' THEN 3 WHEN ride_comp_cd= 'MO' THEN 4 WHEN ride_comp_cd= 'EP' THEN 5 WHEN ride_comp_cd= 'EV' THEN 5 END DESC) RN
    FROM
      RIDER
    WHERE
      ride_CANC_DT IS NULL
      AND (RIDE_EFF_DT IS NOT NULL
        OR RIDE_COMM_CD IN ('T1',
          'T'))
      AND UPPER(ride_STS_CD) <> 'C' ) RDR
  WHERE
    RDR.RN=1 ) R
ON
  R.mbrs_ky=mbr.mbrs_ky
LEFT OUTER JOIN (
  SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY mbr_ky, mbrs_ky ORDER BY spcl_ky DESC) rn
    FROM
      special_handling) sp
  WHERE
    sp.rn=1) sp
ON
  sp.mbr_ky=mbr.mbr_ky
  AND sp.mbrs_ky=mbr.mbrs_ky
INNER JOIN (
  SELECT
    mbrs_ky
  FROM
    mbrship
  WHERE
    mbrship.mbrs_rtd2k_updt_dt > CAST('1900-01-01' AS datetime) UNION
  SELECT
    sq.mbrs_ky AS mbrs_ky
  FROM (
    SELECT
      DISTINCT mbrship_updates.mbrs_ky AS mbrs_ky,
      mbr.mbr_ky AS mbr_ky,
      ROW_NUMBER() OVER (PARTITION BY mbrship_updates.mbrs_ky ORDER BY mbrship_updates.last_updt_dt DESC) AS rn
    FROM
      mbr,
      mbrship_updates
    WHERE
      mbrship_updates.mbrs_ky=mbr.mbrs_ky
      AND mbrship_updates.last_updt_dt > CAST('1900-01-01' AS datetime) ) sq
  WHERE
    sq.rn=1 UNION
  SELECT
    sq.mbrs_ky AS mbrs_ky
  FROM (
    SELECT
      DISTINCT mbrship_comment.mbrs_ky AS mbrs_ky,
      mbr.mbr_ky AS mbr_ky,
      ROW_NUMBER() OVER (PARTITION BY mbr.mbr_ky ORDER BY mbrship_comment.mcmt_updt_dt DESC) AS rn
    FROM
      mbr,
      mbrship_comment
    WHERE
      mbrship_comment.mbrs_ky=mbr.mbrs_ky
      AND mbrship_comment.mcmt_updt_dt > CAST('1900-01-01' AS datetime) ) sq
  WHERE
    sq.rn=1 ) cdc
ON
  cdc.mbrs_ky=mbr.mbrs_ky
WHERE
 YEAR(mbr.mbr_jn_aaa_dt) = v_incr_date