{{config(
    tags=["itm"]
    ,pre_hook=mpre_hook(c_package="ITM", c_status="RUNNING", c_bookmark="AFTER_STG")
    )}}

SELECT 
    ITM_ID,
    ITM_DESC,
    STY_KEY,
    PRD.STY_ID,
    STY_DESC,
    SBC_KEY,
    SBC_ID,
    SBC_DESC,
    CLS_KEY,
    CLS_ID,
    CLS_DESC,
    DPT_KEY,
    DPT_ID,
    DPT_DESC,
    GRP_KEY,
    GRP_ID,
    GRP_DESC,
    DIV_KEY,
    DIV_ID,
    DIV_DESC,
    COMPANY_KEY,
    COMPANY_ID,
    COMPANY_DESC,
    UPC_ID,
    --SUP_KEY,
    --SUP_ID,
    --SUP_PART_ID,
    --SUP_DESC,
    COLOR_KEY,
    PRD.COLOR_ID,
    COLOR_DESC,
    SIZE_KEY,
    PRD.SIZE_ID,
    SIZE_DESC,
    PRD.MER_IND as MER_IND,
    PRD.FCST_IND as FCST_IND,
    PRD.INV_IND as INV_IND,
    PRD.PACK_IND as PACK_IND,
    PRD.ORDERABLE_IND as ORDERABLE_IND,
    PRD.SELLABLE_IND as SELLABLE_IND,
    PRD.SIMPLE_PACK_IND as SIMPLE_PACK_IND,
    PRD.STND_UOM_CDE as STND_UOM_CDE,
    PRD.FIRST_RCVD_DT as FIRST_RCVD_DT,
    PRD.LAST_RCVD_DT as LAST_RCVD_DT,
    PRD.FIRST_SOLD_DT as FIRST_SOLD_DT,
    PRD.LAST_SOLD_DT as LAST_SOLD_DT,
    PRD.RMS_Q_SKU_DNUM as RMS_Q_SKU_DNUM,
    PRD.RMS_Q_SEASON_CDE as RMS_Q_SEASON_CDE,
    PRD.RMS_Q_STY_IMG as RMS_Q_STY_IMG,
    PRD.RMS_Q_STY_IMG_S as RMS_Q_STY_IMG_S,
    PRD.RMS_Q_SKU_SDESC as RMS_Q_SKU_SDESC,
    PRD.RMS_Q_ORIG_UNIT_RTL as RMS_Q_ORIG_UNIT_RTL,
    PRD.RMS_Q_STY_IMG_L as RMS_Q_STY_IMG_L,
    PRD.RMS_ITEM_NBR_TYPE_CDE as RMS_ITEM_NBR_TYPE_CDE,
    PRD.RMS_ITEM_SECND_DESC as RMS_ITEM_SECND_DESC,
    PRD.RMS_LEVEL3_IDNT as RMS_LEVEL3_IDNT,
    PRD.RMS_TRAN_LEVEL as RMS_TRAN_LEVEL,
    PRD.RMS_Q_STATUS_FLAG as RMS_Q_STATUS_FLAG,
    PRD.RMS_SRC_ID as RMS_SRC_ID,
    PRD.RMS_Q_DIM_SNUM as RMS_Q_DIM_SNUM,
    PRD.RMS_Q_VND_SNUM as RMS_Q_VND_SNUM,
    PRD.RMS_Q_SKU_IMG as RMS_Q_SKU_IMG,
    PRD.RMS_LEVEL2_IDNT as RMS_LEVEL2_IDNT,
    PRD.RMS_LOAD_DATE as RMS_LOAD_DATE,
    PRD.RMS_Q_CURR_UNIT_RTL as RMS_Q_CURR_UNIT_RTL,
    PRD.RMS_Q_CURR_PRICE_STATUS as RMS_Q_CURR_PRICE_STATUS

FROM {{ref('V_STG_D_PRD_ITM_LU')}} AS PRD
LEFT OUTER JOIN {{ref('DWH_D_PRD_STY_LU')}} AS STY
ON PRD.STY_ID = STY.STY_ID
LEFT OUTER JOIN {{ref('DWH_D_PRD_COLOR_LU')}} AS COLOR
ON PRD.COLOR_ID = COLOR.COLOR_ID
LEFT OUTER JOIN {{ref('DWH_D_PRD_SIZE_LU')}} AS SIZE
ON PRD.SIZE_ID = SIZE.SIZE_ID