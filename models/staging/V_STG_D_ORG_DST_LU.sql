{{config(
    tags=["dst"]
    ,pre_hook=mpre_hook(c_package="DST", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_ORG_DST_LU  as(
SELECT CASE
           WHEN (((UPPER(S.DISTRICT) || ':') || UPPER(S.REGION)) IS NOT NULL) THEN ((UPPER(S.DISTRICT) || ':') || UPPER(S.REGION))
           ELSE '0'
       END AS DST_ID,
       CASE
           WHEN (S.DIST_DESC IS NOT NULL) THEN S.DIST_DESC
           ELSE 'N/A'
       END AS DST_DESC,
       CASE
           WHEN (S.REGION IS NOT NULL) THEN S.REGION
           ELSE '0'
       END AS RGN_ID,
        '-1' DST_MGR_FIRST_NAME
        ,'-1' DST_MGR_LAST_NAME
        ,'-1' DST_MGR_HOME_STR
        ,'-1' DST_MGR_PHONE
        ,'-1' DST_MGR_MAIL_EXT
        ,'-1' DST_MGR_COUNTRY_CDE
        ,'-1' DST_MGR_ASSISTANT_NAME
        ,'-1' DST_MGR_MOBILE,
/*Extra Columns Not in Mapping*/
       CASE
           WHEN (S.DIST_DESC IS NOT NULL) THEN S.DIST_DESC
           ELSE 'N/A'
       END AS RMS_Q_DST_SDESC,
       CASE
           WHEN (S.DISTRICT IS NOT NULL) THEN S.DISTRICT
           ELSE '9999'
       END AS RMS_Q_DST_DNUM,
       '0' AS RMS_SRC_ID,
       CURRENT_DATE AS RMS_LOAD_DATE  --select *
FROM {{source('DW_LND_RMS','DISTRICT')}} AS S
)

select * from V_STG_D_ORG_DST_LU 