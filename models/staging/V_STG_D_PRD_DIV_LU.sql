
{{config(
    tags=["div"]
   ,pre_hook=mpre_hook(c_package="DIV", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_PRD_DIV_LU as 
(
    SELECT
CASE
           WHEN (S.DIV_IDNT is NOT NULL) THEN S.DIV_IDNT
           else '0'
       END AS DIV_ID  --Q_DMM_SNUM
,CASE
           WHEN (S.DIV_DESC is not NULL) THEN S.DIV_DESC
           else 'N/A'
       END AS DIV_DESC  --Q_DMM_SDESC
,CASE
           WHEN (S.CMPY_IDNT is NOT NULL) THEN S.CMPY_IDNT
           ELSE '0'
       END AS COMPANY_ID  --CMPY_IDNT
/* Extra columns in mapping */
,CASE
           WHEN (S.DIV_IDNT is NOT NULL) THEN S.DIV_IDNT
           ELSE '0'
       END AS RMS_Q_DMM_DNUM,
       CASE
           WHEN ("SUBSTRING"(S.DIV_DESC,
                             1,
                             15) is NOT NULL) THEN "SUBSTRING"(S.DIV_DESC,
                                                           1,
                                                           15)
           ELSE 'N/A'
       END AS RMS_Q_DMM_SDESC,
       CASE
           WHEN (S.DIV_BUYR_IDNT is NOT NULL) THEN S.DIV_BUYR_IDNT
           ELSE '0'
       END AS RMS_DIV_BYR_IDNT,
       CASE
           WHEN (S.DIV_BUYR_NAME is NOT NULL) THEN S.DIV_BUYR_NAME
           ELSE '0'
       END AS RMS_DIV_BYR_NAME,
       CASE
           WHEN (S.DIV_MRCH_IDNT is NOT NULL) THEN S.DIV_MRCH_IDNT
           ELSE '0'
       END AS RMS_DIV_MRCH_IDNT,
       CASE
           WHEN (S.DIV_MRCH_NAME is NOT NULL) THEN S.DIV_MRCH_NAME
           ELSE '0'
       END AS RMS_DIV_MRCH_NAME,
       '0' AS RMS_SRC_ID
       ,CURRENT_DATE AS RMS_LOAD_DATE
FROM {{source('DW_LND_RMS','DIVISION')}} AS S
WHERE S.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale')
)

select * from V_STG_D_PRD_DIV_LU