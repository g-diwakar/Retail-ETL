
{{config(
    tags=["color"]
   ,pre_hook=mpre_hook(c_package="COLOR", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_PRD_COLOR_LU as
(
SELECT CASE
           WHEN (S.DIFF_IDNT IS NOT NULL) THEN S.DIFF_IDNT
           ELSE '0'
       END AS COLOR_ID,
       CASE
           WHEN (S.DIFF_DESC IS  NOT NULL) THEN S.DIFF_DESC
           ELSE 'N/A'
       END AS COLOR_DESC,
       CASE
           WHEN (S.INDUSTRY_CDE IS NOT NULL) THEN S.INDUSTRY_CDE
           ELSE '0'
       END AS COLOR_CIN_CDE,
       CASE
           WHEN ("SUBSTRING"(S.DIFF_DESC,1,15) IS NOT NULL) THEN "SUBSTRING"(S.DIFF_DESC,1,15)
           ELSE 'N/A'
       END AS COLOR_FAMILY_DESC, --RMS_Q_CLR_SDESC

	   -- /*EXTRA COLUMNS IN MAPPING*/
       CASE
           WHEN (S.DIFF_IDNT IS NOT NULL) THEN S.DIFF_IDNT
           ELSE '0'
       END AS RMS_Q_CLR_DNUM,
       CASE
           WHEN (S.DIFF_TYPE IS NOT NULL) THEN S.DIFF_TYPE
           ELSE '0'
       END AS RMS_DIFF_TYPE,
       '0' AS RMS_SRC_ID--SELECT *
FROM {{source('DW_LND_RMS','PRDDIFFDM')}} AS s
)

select * from V_STG_D_PRD_COLOR_LU  