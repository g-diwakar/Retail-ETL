
{{config(
    tags=["cls"]
    ,pre_hook=mpre_hook(c_package="CLS", c_status="RUNNING", c_bookmark="AFTER_STG")
)}}

SELECT
    CLS_ID,
    DPT.DPT_ID as DPT_ID,
    CLS_DESC,
    DPT.DPT_KEY as DPT_KEY,
    DPT.DPT_DESC as DPT_DESC,
    DPT.GRP_KEY as GRP_KEY,
    DPT.GRP_ID as GRP_ID,
    DPT.GRP_DESC as GRP_DESC,
    DPT.DIV_KEY as DIV_KEY,
    DPT.DIV_ID as DIV_ID,
    DPT.DIV_DESC as DIV_DESC,
    DPT.COMPANY_KEY as COMPANY_KEY,
    DPT.COMPANY_ID as COMPANY_ID,
    DPT.COMPANY_DESC as COMPANY_DESC,
    RMS_CLASS_MRCH_NAME,
    RMS_Q_CLS_SDESC,
    CLS.RMS_LOAD_DATE as RMS_LOAD_DATE,
    RMS_CLASS_MRCH_IDNT,
    CLS.RMS_SRC_ID as RMS_SRC_ID,
    RMS_CLASS_BUYR_SNUM,
    RMS_Q_CLS_DNUM,
    RMS_CLASS_BUYR_NAME

FROM {{ref('V_STG_D_PRD_CLS_LU')}} as CLS
LEFT OUTER JOIN {{ref('DWH_D_PRD_DPT_LU')}} as DPT
ON CLS.DPT_ID = DPT.DPT_ID