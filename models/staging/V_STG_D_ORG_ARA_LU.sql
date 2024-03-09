{{config(
	tags=["ara"]
    ,pre_hook=mpre_hook(c_package="ARA", c_status="RUNNING", c_bookmark="NONE")
	)}}


with V_STG_D_ORG_ARA_LU as 
(
SELECT CASE
           WHEN (S.AREA IS NOT NULL) THEN S.AREA
           ELSE 'o'
       END AS ARA_ID, --Q_CPY_SNUM,

       CASE
           WHEN (S.AREA_DESC IS NOT NULL) THEN S.AREA_DESC
           ELSE '0'
       END AS ARA_DESC, --Q_CPY_DESC,
	  '-1' AS CHN_KEY ,
	  '-1' AS CHN_ID ,
	  '-1' AS CHN_DESC,
	  '-1' AS COMPANY_KEY,
	  '-1' AS COMPANY_ID,
	  '-1' AS COMPANY_DESC,
	  '-1' AS ARA_MGR_FIRST_NAME,
	  '-1' AS ARA_MGR_LAST_NAME,
	  '-1' AS ARA_MGR_HOME_STR,
	  '-1' AS ARA_MGR_PHONE,
	  '-1' AS ARA_MGR_MAIL_EXT,
	  '-1' AS ARA_MGR_COUNTRY_CDE,
	  '-1' AS ARA_MGR_ASSISTANT_NAME,
	  '-1' AS ARA_MGR_MOBILE,

	   ---- /*EXTRA COLUMNS IN MAPPING*/
	   CASE
           WHEN (S.AREA IS NOT NULL) THEN S.AREA
           ELSE '0'
       END AS RMS_Q_CPY_DNUM,
       CASE
           WHEN (S.AREA_DESC IS NOT NULL) THEN S.AREA_DESC
           ELSE '0'
       END AS RMS_Q_CPY_SDESC,
       '0' AS SRC_ID
FROM {{source('DW_LND_RMS','AREA')}} AS S
)

select * from V_STG_D_ORG_ARA_LU