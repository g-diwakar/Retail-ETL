
{{config(
    tags=["itm"]
   ,pre_hook=mpre_hook(c_package="ITM", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_PRD_ITM_LU as 
(
SELECT CASE
           WHEN (S.ITEM_IDNT IS NOT NULL) THEN S.ITEM_IDNT
           ELSE '0'
       END AS ITM_ID,  --Q_SKU_SNUM
       CASE
           WHEN (S.ITEM_DESC IS NOT NULL) THEN S.ITEM_DESC
           ELSE 'N/A'
       END AS ITM_DESC,  --Q_SKU_DESC
       S.LEVEL1_IDNT AS STY_ID,  --Q_ITM_SNUM
       CASE
           WHEN (S.DIFF_1 IS NOT NULL) THEN S.DIFF_1
           ELSE '0'
       END AS COLOR_ID, --Q_CLR_SNUM
       CASE
           WHEN (S.DIFF_2 IS NOT NULL) THEN S.DIFF_2
           ELSE '0'
       END AS SIZE_ID, --Q_SIZ_SNUM
        CASE
            WHEN (S.PACK_IND IS NOT NULL) THEN S.PACK_IND
            ELSE '0'
        END AS PACK_IND,
        CASE
            WHEN (S.PACK_ORDERABLE_CDE IS NOT NULL) THEN S.PACK_ORDERABLE_CDE
            ELSE '0'
        END AS ORDERABLE_IND,
        '0' AS FIRST_RCVD_DT,
        '0' AS LAST_RCVD_DT,
        CASE
            WHEN (S.PACK_SIMPLE_CDE IS NOT NULL) THEN S.PACK_SIMPLE_CDE
            ELSE '0'
        END AS SIMPLE_PACK_IND,
        '-1' AS UPC_ID,
        '-1' AS SUP_ID,
        '-1' AS SUP_PART_ID,
        '-1' AS MER_IND,
        '-1' AS FCST_IND,
        '-1' AS INV_IND,
        '-1' AS SELLABLE_IND,
        '-1' AS STND_UOM_CDE,
        '-1' AS FIRST_SOLD_DT,
        '-1' AS LAST_SOLD_DT,
/*Extra columns in Mapping*/
       NULL AS RMS_Q_SKU_IMG,
        CASE
            WHEN ("SUBSTRING"(S.ITEM_DESC,1,15) IS NOT NULL) THEN "SUBSTRING"(S.ITEM_DESC,1,15)
            ELSE 'N/A'
        END AS RMS_Q_SKU_SDESC,
        CASE
            WHEN (S.ITEM_IDNT IS NOT NULL) THEN S.ITEM_IDNT
            ELSE '9999'
        END AS RMS_Q_SKU_DNUM,
        '0' AS RMS_Q_DIM_SNUM,
        'N/A' AS RMS_Q_STY_IMG,
        'N/A' AS RMS_Q_STY_IMG_S,
        'N/A' AS RMS_Q_STY_IMG_L,
        CASE
            WHEN ('0' IS NOT NULL) THEN '0'
            WHEN ('0' IS NOT NULL) THEN '0'
            ELSE NULL
        END AS RMS_Q_AST_SNUM,
        CASE
            WHEN ('0' IS NOT NULL) THEN '0'
            WHEN ('0' IS NOT NULL) THEN '0'
            ELSE NULL
        END AS RMS_Q_VND_SNUM,
        '0' AS RMS_Q_CURR_UNIT_RTL,
        '0' AS RMS_Q_ORIG_UNIT_RTL,
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
        '0' AS RMS_SRC_ID,
        CURRENT_DATE AS RMS_LOAD_DATE
FROM {{source('DW_LND_RMS','ITEM_MASTER')}} AS S
LEFT JOIN {{source('DW_LND_RMS','DEPARTMENT')}} AS U ON S.DEPT_IDNT = U.DEPT_IDNT
LEFT JOIN {{source('DW_LND_RMS','PROD_GROUP')}} AS V ON U.GRP_IDNT = V.GRP_IDNT
LEFT JOIN {{source('DW_LND_RMS','DIVISION')}} AS D ON D.DIV_IDNT = V.DIV_IDNT
WHERE ((S.ITEM_LEVEL = S.TRAN_LEVEL) AND (S.TRAN_LEVEL = '2') AND D.DIV_DESC IN ('NexusNova', 'NexusNova Non Sale'))
)

select * from V_STG_D_PRD_ITM_LU