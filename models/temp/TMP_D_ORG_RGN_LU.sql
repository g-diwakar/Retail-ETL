{{config(
    tags=["rgn"]
    ,pre_hook=mpre_hook(c_package="RGN", c_status="RUNNING", c_bookmark="AFTER_STG")
    )}}

SELECT 
    RGN_ID,
    RGN_DESC,
    ARA.ARA_KEY as ARA_KEY,
    ARA.ARA_ID as ARA_ID,
    ARA.ARA_DESC as ARA_DESC,
    ARA.CHN_KEY as CHN_KEY,
    ARA.CHN_ID as CHN_ID,
    ARA.CHN_DESC as CHN_DESC,
    ARA.COMPANY_KEY as COMPANY_KEY,
    ARA.COMPANY_ID as COMPANY_ID,
    ARA.COMPANY_DESC as COMPANY_DESC,
    RGN_MGR_FIRST_NAME,
    RGN_MGR_LAST_NAME,
    RGN_MGR_HOME_STR,
    RGN_MGR_PHONE,
    RGN_MGR_MAIL_EXT,
    RGN_MGR_ASSISTANT_NAME,
    RGN_MGR_MOBILE,
    RMS_Q_RGN_SDESC,
    RMS_SRC_ID,
    RMS_Q_RGN_DNUM,
    RMS_LOAD_DATE

FROM {{ref('V_STG_D_ORG_RGN_LU')}} AS RGN
LEFT OUTER JOIN {{ref('DWH_D_ORG_ARA_LU')}} as ARA on RGN.ARA_ID = ARA.ARA_ID 