SELECT 
	CONVERT(char, auth_run_hist_ky) AS auth_run_hist_ky,
	CONVERT(char, mbrs_ky) AS mbrs_ky,
	CONVERT(char, attempt) AS attempt,
	CONVERT(char, e_pmt_hist_ky) AS e_pmt_hist_ky,
	CONVERT(char, ran_dt) AS ran_dt,
	CONVERT(char, bpmt_ky) AS bpmt_ky
FROM m_prd.dbo.auth_recycle_run_hist
WHERE CONVERT(DATE,ran_dt) >= DATEADD(day,-1,convert(date, 'v_inputdate'))