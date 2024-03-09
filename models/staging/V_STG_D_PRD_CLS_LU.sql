{{config(
    tags=["cls"]
   ,pre_hook=mpre_hook(c_package="CLS", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_PRD_CLS_LU as 
(
SELECT
		CASE
           WHEN (((S.DEPT_IDNT || '-') || S.CLASS_IDNT) IS NOT NULL)
		   THEN ((S.DEPT_IDNT || '-') || S.CLASS_IDNT)
           ELSE '0'
        END AS CLS_ID,

	   CASE
           WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
           ELSE '0'
       END AS DPT_ID,

		CASE
           WHEN (S.CLASS_DESC IS NOT NULL) THEN S.CLASS_DESC
           ELSE 'N/A'
       END AS CLS_DESC,

		CASE
           WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
           ELSE '0'
		END AS Q_DPT_SNUM,

/* Extra unmapped columns*/

		CASE
           WHEN ("SUBSTRING"(S.CLASS_DESC,
                             1,
                             15) IS NOT NULL) THEN "SUBSTRING"(S.CLASS_DESC,
                                                           1,
                                                           15)
           ELSE 'N/A'
		END AS RMS_Q_CLS_SDESC,

		CASE
           WHEN (((S.DEPT_IDNT || '-') || S.CLASS_IDNT) IS NOT NULL)
		   THEN ((S.DEPT_IDNT || '-') || S.CLASS_IDNT)
           ELSE '9999'
		END AS RMS_Q_CLS_DNUM,

		CASE
           WHEN (S.CLASS_BUYR_IDNT IS NOT NULL) THEN S.CLASS_BUYR_IDNT
           ELSE '0'
		END AS RMS_CLASS_BUYR_SNUM,

		CASE
           WHEN (S.CLASS_BUYR_NAME IS NOT NULL) THEN S.CLASS_BUYR_NAME
           ELSE 'N/A'
       END AS RMS_CLASS_BUYR_NAME,

	   CASE
           WHEN (S.CLASS_MRCH_IDNT IS NOT NULL) THEN S.CLASS_MRCH_IDNT
           ELSE '0'
       END AS RMS_CLASS_MRCH_IDNT,

	   CASE
           WHEN (S.CLASS_MRCH_NAME IS NOT NULL) THEN S.CLASS_MRCH_NAME
           ELSE 'N/A'
       END AS RMS_CLASS_MRCH_NAME,

		'0' AS RMS_SRC_ID,
		CURRENT_DATE AS RMS_LOAD_DATE

FROM {{source('DW_LND_RMS','CLASS')}} AS S
LEFT JOIN {{source('DW_LND_RMS','DEPARTMENT')}} AS D ON S.DEPT_IDNT = D.DEPT_IDNT
LEFT JOIN {{source('DW_LND_RMS','PROD_GROUP')}} AS P ON D.GRP_IDNT = P.GRP_IDNT
LEFT JOIN {{source('DW_LND_RMS','DIVISION')}} AS T ON P.DIV_IDNT = T.DIV_IDNT
WHERE T.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale')
)

select * from V_STG_D_PRD_CLS_LU