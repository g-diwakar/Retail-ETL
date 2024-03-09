{{config(
   tags=["sbc"]
   ,pre_hook=mpre_hook(c_package="SBC", c_status="RUNNING", c_bookmark="NONE")
   )}}

with V_STG_D_PRD_SBC_LU as
(
select
/*Mapping scripts*/
    CASE
       WHEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT IS NOT NULL )
       THEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT)
       ELSE '0'
    END As SBC_ID,
    CASE
       WHEN (S.SBCLASS_DESC IS NOT NULL) THEN S.SBCLASS_DESC
       ELSE 'N/A'
    END AS SBC_DESC,
    CASE
       WHEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT  IS NOT NULL )
       THEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT  )
       ELSE '0'
    END AS CLS_ID,
    CASE
       WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
       ELSE '0'
    END AS DPT_ID,
    PGRP.GRP_IDNT  AS GRP_ID,
    /*Extra unmapped columns*/
    CASE
       WHEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT IS NOT NULL)
       THEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT)
       ELSE '9999'
    END AS RMS_Q_SBC_DNUM,
    CASE
       WHEN ("SUBSTRING"(S.SBCLASS_DESC,1,15) IS NOT NULL)
        THEN "SUBSTRING"(S.SBCLASS_DESC,1,15)
       ELSE 'N/A'
    END AS RMS_Q_SBC_SDESC,
    CASE
       WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
       ELSE '0'
    END AS RMS_Q_DPT_SNUM,
   'N/A' AS RMS_Q_IMG_DFLT,
   'N/A' AS RMS_Q_IMG_SM,
   'N/A' AS RMS_Q_IMG_LG,
    CASE
       WHEN (S.SBCLASS_BUYR_IDNT IS NOT NULL) THEN S.SBCLASS_BUYR_IDNT
       ELSE '0'
    END AS RMS_SBCLASS_BUYR_IDNT,
    CASE
       WHEN (S.SBCLASS_BUYR_NAME IS NOT NULL) THEN S.SBCLASS_BUYR_NAME
       ELSE 'N/A'
    END AS RMS_SBCLASS_BUYR_NAME,
    CASE
       WHEN (S.SBCLASS_MRCH_IDNT IS NOT NULL) THEN S.SBCLASS_MRCH_IDNT
       ELSE '0'
    END AS RMS_SBCLASS_MRCH_IDNT,
    CASE
       WHEN (S.SBCLASS_MRCH_NAME IS NOT NULL) THEN S.SBCLASS_MRCH_NAME
       ELSE 'N/A'
    END AS RMS_SBCLASS_MRCH_NAME,
    '0' AS RMS_SRC_ID,
     CURRENT_DATE AS RMS_LOAD_DATE
FROM {{source('DW_LND_RMS','SUBCLASS')}} AS S
LEFT JOIN {{source('DW_LND_RMS','CLASS')}} AS CLS ON S.CLASS_IDNT = S.CLASS_IDNT
LEFT JOIN {{source('DW_LND_RMS','DEPARTMENT')}} AS  DPT ON DPT.DEPT_IDNT=S.DEPT_IDNT
LEFT JOIN {{source('DW_LND_RMS','PROD_GROUP')}} AS PGRP ON PGRP.GRP_IDNT=DPT.GRP_IDNT
LEFT JOIN {{source('DW_LND_RMS','DIVISION')}} AS DIV ON DIV.DIV_IDNT=PGRP.DIV_IDNT
WHERE DIV.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale')
)

select * from V_STG_D_PRD_SBC_LU 