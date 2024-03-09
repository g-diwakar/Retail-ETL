{{config(
    tags=["dpt"]
   ,pre_hook=mpre_hook(c_package="DPT", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_PRD_DPT_LU as (
    SELECT
        CASE
           WHEN (S.DEPT_IDNT is NOT NULL) THEN S.DEPT_IDNT
           ELSE NULL
		END AS DPT_ID,

       CASE
           WHEN (S.DEPT_DESC IS NOT NULL) THEN S.DEPT_DESC
           ELSE NULL
       END AS DPT_DESC,

       CASE
          WHEN (S.GRP_IDNT IS NOT NULL) THEN S.GRP_IDNT
		  ELSE NULL
	   END AS GRP_ID,

/* Extra unmapped columns*/

		CASE
           WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
           ELSE '9999'
		END AS RMS_Q_DPT_DNUM,

		CASE
           WHEN ("SUBSTRING"(S.DEPT_DESC,
                             1,
                             15) IS NOT NULL) THEN "SUBSTRING"(S.DEPT_DESC,
                                                           1,
                                                           15)
           ELSE 'N/A'
        END AS RMS_Q_DPT_SDESC,

		CASE
           WHEN (S.DEPT_BUYR_NAME IS NOT NULL) THEN S.DEPT_BUYR_NAME
           ELSE 'N/A'
		END AS RMS_Q_DPT_BYR_NM,

		CASE
           WHEN (S.PURCH_TYPE_CDE IS NOT NULL) THEN S.PURCH_TYPE_CDE
           ELSE 'N/A'
		END AS RMS_PURCH_TYPE_CDE,

		CASE
           WHEN (S.PRFT_CALC_TYPE_CDE IS NOT NULL) THEN S.PRFT_CALC_TYPE_CDE
           ELSE 'N/A'
        END AS RMS_PRFT_CALC_TYPE_CDE,

		CASE
           WHEN (S.MKUP_CALC_TYPE_CDE IS NOT NULL) THEN S.MKUP_CALC_TYPE_CDE
           ELSE 'N/A'
		END AS RMS_MKUP_CALC_TYPE_CDE,

		CASE
           WHEN (S.OTB_CALC_TYPE_CDE IS NOT NULL) THEN S.OTB_CALC_TYPE_CDE
           ELSE 'N/A'
		END AS RMS_OTB_CALC_TYPE_CDE,

		CASE
           WHEN (S.BUD_INT IS NOT NULL) THEN S.BUD_INT
           ELSE '9999'
		END AS RMS_BUD_INT,

		CASE
           WHEN (S.BUD_MKUP IS NOT NULL) THEN S.BUD_MKUP
           ELSE '9999'
		END AS RMS_BUD_MKUP,

		'0' AS RMS_SRC_ID,
		CURRENT_DATE AS RMS_LOAD_DATE

FROM {{ source('DW_LND_RMS', 'DEPARTMENT') }} AS S
LEFT JOIN {{source('DW_LND_RMS','PROD_GROUP')}} AS P ON S.GRP_IDNT = P.GRP_IDNT
LEFT JOIN {{ source('DW_LND_RMS','DIVISION')}} AS T ON P.DIV_IDNT = T.DIV_IDNT
WHERE T.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale')
)

SELECT * FROM V_STG_D_PRD_DPT_LU
