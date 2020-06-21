INSERT INTO OPERATIONAL.AUTH_RECYCLE_RUN_HIST
SELECT 
	auth_run_hist_ky,
	mbrs_ky,
	attempt,
	e_pmt_hist_ky,
	ran_dt,
	bpmt_ky
FROM LANDING.AUTH_RECYCLE_RUN_HIST;