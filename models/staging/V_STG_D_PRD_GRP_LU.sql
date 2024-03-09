
{{config(
    tags=["grp"]
   ,pre_hook=mpre_hook(c_package="GRP", c_status="RUNNING", c_bookmark="NONE")
    )}}
with V_STG_D_PRD_GRP_LU as 
(
SELECT
CASE
           WHEN (S.GRP_IDNT IS NOT NULL) THEN S.GRP_IDNT
           ELSE 0
       END AS GRP_ID,  --Q_BYR_DESC
CASE
           WHEN (S.GRP_DESC IS NOT NULL) THEN S.GRP_DESC
           ELSE 'N/A'
       END AS GRP_DESC,  --Q_BYR_DESC
CASE
           WHEN (S.DIV_IDNT IS NOT NULL) THEN S.DIV_IDNT
           ELSE 0
       END AS DIV_ID,  --Q_DMM_SNUM
/*EXTRA COLUMNS IN MAPPING*/
CASE
           WHEN (S.GRP_BUYR_IDNT IS NOT NULL) THEN S.GRP_BUYR_IDNT
           ELSE 0
       END AS RMS_GRP_BUYR_IDNT,
CASE
           WHEN (S.GRP_BUYR_NAME IS NOT NULL) THEN S.GRP_BUYR_NAME
           ELSE 'N/A'
       END AS RMS_GRP_BUYR_NAME,
CASE
           WHEN (S.GRP_MRCH_IDNT IS NOT NULL) THEN S.GRP_MRCH_IDNT
           ELSE 'N/A'
       END AS RMS_GRP_MRCH_IDNT,
CASE
           WHEN (S.GRP_MRCH_NAME IS NOT NULL) THEN S.GRP_MRCH_NAME
           ELSE 'N/A'
       END AS RMS_GRP_MRCH_NAME,
       '0' AS RMS_SRC_ID,
CASE
           WHEN (S.GRP_IDNT IS NOT NULL) THEN S.GRP_IDNT
           ELSE 9999
       END AS RMS_Q_BYR_DNUM,
CASE
           WHEN ("SUBSTRING"(S.GRP_DESC,
                             1,
                             15) IS NOT NULL) THEN "SUBSTRING"(S.GRP_DESC,
                                                           1,
                                                           15)
           ELSE 'N/A'
       END AS RMS_Q_BYR_SDESC,
       CURRENT_DATE AS RMS_LOAD_DATE
FROM {{source('DW_LND_RMS','PROD_GROUP')}} AS S
LEFT JOIN {{source('DW_LND_RMS','DIVISION')}} AS D ON D.DIV_IDNT = S.DIV_IDNT
WHERE D.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale')
)

select * from V_STG_D_PRD_GRP_LU  