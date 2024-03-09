
{{config(
  tags=["chn"]
  ,pre_hook=mpre_hook(c_package="CHN", c_status="RUNNING", c_bookmark="NONE")
  )}}

with V_STG_D_ORG_CHN_LU as
(
SELECT
  NVL(BRN.BRAND, '0') 		           AS CHN_ID,
  NVL(TRIM(BRN.BRAND_DESC), 'N/A')     AS CHN_DESC,
  -1                                  AS COMPANY_ID,
  NULL                                 AS CHN_MGR_FIRST_NAME,
  NULL                                 AS CHN_MGR_LAST_NAME,
  NULL                                 AS CHN_MGR_HOME_STR,
  NULL                                 AS CHN_MGR_PHONE,
  NULL                                 AS CHN_MGR_MAIL_EXT,
  NULL                                 AS CHN_MGR_COUNTRY_CDE,
  NULL                                 AS CHN_MGR_ASSISTANT_NAME,
  NULL                                 AS CHN_MGR_MOBILE,
  /* FLEX FIELDS COULUMN */
  NVL(BRN.BRAND, '0')                  AS RMS_BRND_SNUM,
  NVL(BRN.BRAND, '9999')               AS RMS_BRND_DNUM,
  NVL(BRN.BRAND_DESC, 'N/A')           AS RMS_BRND_DESC,
  NVL(BRN.BRAND_DESC, 'N/A')           AS RMS_BRND_SDESC,
  '0'                                  AS RMS_SRC_ID
FROM {{source('DW_LND_RMS','BRAND')}} AS BRN
  WHERE BRN.BRAND IN ('NexusNova', 'DC')
)

select * from V_STG_D_ORG_CHN_LU