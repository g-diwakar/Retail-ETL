
{{config(
    tags=["rgn"]
   ,pre_hook=mpre_hook(c_package="RGN", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_ORG_RGN_LU as
(
SELECT
        CASE
            WHEN (S.REGION IS NOT NULL) THEN S.REGION
            ELSE '0'
        END AS RGN_ID,

        CASE
            WHEN (S.REGION_DESC IS NOT NULL) THEN S.REGION_DESC
            ELSE 'N/A'
        END AS RGN_DESC,

        CASE
            WHEN (S.AREA IS NOT NULL) THEN S.AREA
            ELSE '0'
        END AS ARA_ID,

        '-1' AS RGN_MGR_FIRST_NAME,
        '-1' AS RGN_MGR_LAST_NAME,
        '-1' AS RGN_MGR_HOME_STR,
        '-1' AS RGN_MGR_PHONE,
        '-1' AS RGN_MGR_MAIL_EXT,
        '-1' AS RGN_MGR_COUNTRY_CDE,
        '-1' AS RGN_MGR_ASSISTANT_NAME,
        '-1' AS RGN_MGR_MOBILE,

/* Extra unmapped columns*/

        CASE
            WHEN (S.REGION_DESC IS NOT NULL) THEN S.REGION_DESC
            ELSE 'N/A'
        END AS RMS_Q_RGN_SDESC,

        CASE
            WHEN (S.REGION IS NOT NULL) THEN S.REGION
            ELSE '9999'
        END AS RMS_Q_RGN_DNUM,

        '0' AS RMS_SRC_ID,
        CURRENT_DATE AS RMS_LOAD_DATE

FROM {{source('DW_LND_RMS','REGION')}} AS S
)

select * from V_STG_D_ORG_RGN_LU