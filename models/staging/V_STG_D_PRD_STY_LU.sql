{{config(
   tags=["sty"]
   ,pre_hook=mpre_hook(c_package="STY", c_status="RUNNING", c_bookmark="NONE")
)}}

with V_STG_D_PRD_STY_LU as 
(
SELECT
       CASE
          WHEN (S.ITEM_IDNT IS NOT NULL) THEN S.ITEM_IDNT
          ELSE '0'
       END AS STY_ID,
       CASE
          WHEN (S.ITEM_DESC IS NOT NULL) THEN S.ITEM_DESC
          ELSE 'N/A'
       END AS STY_DESC,
       CASE
       WHEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT IS NOT NULL )
       THEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT || '-' || S.SBCLASS_IDNT)
       ELSE '0'
    END As SBC_ID,
       CASE
       WHEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT  IS NOT NULL )
       THEN (S.DEPT_IDNT || '-' || S.CLASS_IDNT  )
       ELSE '0'
    END AS CLS_ID,
    CASE
       WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
       ELSE '0'
    END AS DPT_ID,
       GRP.GRP_IDNT AS GRP_ID,
       DIV.DIV_IDNT AS DIV_ID,
       DIV.CMPY_IDNT AS COMPANY_ID,
       -1 as SUP_ID
        , -1 SUP_KEY
        ,'' as SUP_DESC
        /*,SRC.SUP_PART_ID*/
        ,'' as STND_UOM_CDE
        ,'' as MER_IND
        ,'' as FCST_IND
        ,'' as INV_IND
        ,'' as PACK_IND
        ,'' as SIMPLE_PACK_IND
        ,'' as ORDERABLE_IND
        ,'' as SELLABLE_IND
        ,CURRENT_TIMESTAMP() as FIRST_RCVD_DT
        ,CURRENT_TIMESTAMP() as LAST_RCVD_DT,
        /*Extra unmapped columns*/
        CASE
          WHEN ("SUBSTRING"(s.item_desc, 1, 15) is not null) THEN "SUBSTRING"(s.item_desc, 1, 15)
          ELSE 'N/A'
        END   AS RMS_Q_STY_SDESC,
       'N/A' AS RMS_Q_STY_IMG,
       'N/A' AS RMS_Q_STY_IMG_S,
       'N/A' AS RMS_Q_STY_IMG_L,
       CASE
          WHEN (S.DEPT_IDNT|| '-'|| S.CLASS_IDNT || '-'|| S.SBCLASS_IDNT IS NOT NULL) THEN (S.DEPT_IDNT|| '-'||                     S.CLASS_IDNT || '-'|| S.SBCLASS_IDNT)
          ELSE '0'
       END AS RMS_Q_SBC_SNUM,
       '0' AS RMS_Q_AST_SNUM,
       '0' AS RMS_Q_CURR_UNIT_RTL,
       '0' AS RMS_Q_ORIG_UNIT_RTL,
       '0' AS RMS_Q_FIRST_RECPT_DT,
       '0' AS RMS_Q_LAST_RECPT_DT,
       '0' AS RMS_Q_STATUS_FLAG,
       '0' AS RMS_Q_CURR_PRICE_STATUS,
       '0' AS RMS_Q_SEASON_CDE,
       CASE
          WHEN (S.LEVEL2_IDNT IS NOT NULL) THEN S.LEVEL2_IDNT
          ELSE '0'
       END AS RMS_LEVEL2_IDNT,
       CASE
          WHEN (S.LEVEL3_IDNT IS NOT NULL) THEN S.LEVEL3_IDNT
          ELSE '0'
       END AS RMS_LEVEL3_IDNT,
       CASE
          WHEN (S.DEPT_IDNT IS NOT NULL) THEN S.DEPT_IDNT
          ELSE '0'
       END AS RMS_DEPT_IDNT,
       CASE
          WHEN (S.DEPT_IDNT|| '-'|| S.CLASS_IDNT IS NOT NULL) THEN (S.DEPT_IDNT|| '-'|| S.CLASS_IDNT)
          ELSE '0'
       END AS RMS_CLASS_IDNT,
       CASE
          WHEN (S.DEPT_IDNT|| '-'|| S.CLASS_IDNT|| '-'|| S.SBCLASS_IDNT IS NOT NULL) THEN (S.DEPT_IDNT|| '-'||                      S.CLASS_IDNT|| '-'|| S.SBCLASS_IDNT)
          ELSE '0'
       END AS RMS_SBCLASS_IDNT,
       CASE
          WHEN (S.ITEM_LEVEL IS NOT NULL) THEN S.ITEM_LEVEL
          ELSE '0'
       END AS RMS_ITEM_LEVEL,
       CASE
          WHEN (S.TRAN_LEVEL IS NOT NULL) THEN S.TRAN_LEVEL
          ELSE '0'
       END AS RMS_TRAN_LEVEL,
       CASE
          WHEN (S.ITEM_SECND_DESC IS NOT NULL) THEN S.ITEM_SECND_DESC
          ELSE '0'
       END AS RMS_ITEM_SECND_DESC,
       CASE
          WHEN (S.ITEM_NBR_TYPE_CDE IS NOT NULL) THEN S.ITEM_NBR_TYPE_CDE
          ELSE '0'
       END AS RMS_ITEM_NBR_TYPE_CDE,
       CASE
          WHEN (S.STND_UOM_CDE IS NOT NULL) THEN S.STND_UOM_CDE
          ELSE '0'
       END AS RMS_STND_UOM_CDE,
       CASE
          WHEN (S.PACK_IND IS NOT NULL) THEN S.PACK_IND
          ELSE '0'
       END AS RMS_PACK_IND,
       CASE
          WHEN (S.PACK_SIMPLE_CDE IS NOT NULL) THEN S.PACK_SIMPLE_CDE
          ELSE '0'
       END AS RMS_PACK_SIMPLE_CDE,
       CASE
          WHEN ( S.PACK_ORDERABLE_CDE IS NOT NULL) THEN S.PACK_ORDERABLE_CDE
          ELSE '0'
        END   AS RMS_PACK_ORDERABLE_CDE,
       '0'                       AS RMS_SRC_ID,
       CURRENT_DATE AS RMS_LOAD_DATE
    --SELECT *
FROM {{source('DW_LND_RMS','ITEM_MASTER')}} AS S
LEFT JOIN {{source('DW_LND_RMS','SUBCLASS')}} AS SBC ON SBC.SBCLASS_IDNT=S.SBCLASS_IDNT AND SBC.CLASS_IDNT=S.CLASS_IDNT AND S.DEPT_IDNT=SBC.DEPT_IDNT
LEFT JOIN {{source('DW_LND_RMS','DEPARTMENT')}} AS DPT ON DPT.DEPT_IDNT=SBC.DEPT_IDNT AND SBC.CLASS_IDNT=S.CLASS_IDNT
LEFT JOIN {{source('DW_LND_RMS','PROD_GROUP')}} AS GRP ON GRP.GRP_IDNT=DPT.GRP_IDNT
LEFT JOIN {{source('DW_LND_RMS','DIVISION')}} AS DIV ON DIV.DIV_IDNT=GRP.DIV_IDNT
WHERE  (
(S.ITEM_LEVEL < S.TRAN_LEVEL)
AND    (S.ITEM_LEVEL = '1')
AND    (S.ITEM_IDNT  NOT LIKE '9%'  ))
AND DIV.DIV_DESC IN  ('NexusNova', 'NexusNova Non Sale')
) 

select * from V_STG_D_PRD_STY_LU 