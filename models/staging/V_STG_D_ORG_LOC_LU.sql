{{config(
    tags=["loc"]
   ,pre_hook=mpre_hook(c_package="LOC", c_status="RUNNING", c_bookmark="NONE")
    )}}

with V_STG_D_ORG_LOC_LU as
(
SELECT CASE
           WHEN (S.STORE_NO IS NOT NULL) THEN CAST(S.STORE_NO AS NUMBER)  
           ELSE 9999
       END AS LOC_ID, --Q_STR_SNUM
       CASE
           WHEN (S.STORE_NAME IS NOT NULL) THEN S.STORE_NAME
           ELSE 'N/A'
       END AS LOC_DESC, --Q_STR_DESC
       CASE
           WHEN (S.OPEN_DATE IS NOT NULL) THEN S.OPEN_DATE
           ELSE '1900-01-01'
       END AS LOC_START_DT,  --Q_STR_OPN_DT
       CASE
           WHEN (S.CLOSE_DATE IS NOT NULL) THEN S.CLOSE_DATE
           ELSE '1900-01-01'
       END AS LOC_END_DT,  --Q_STR_CLOSE_DT
       CASE
           WHEN (((UPPER(S.DISTRICT) || ':') || UPPER(S.REGION)) IS NOT NULL) THEN ((UPPER(S.DISTRICT) || ':') || UPPER(S.REGION))
           ELSE '0'
       END AS DST_ID, --Q_DST_SNUM
       CASE
           WHEN (S.STORE_FORMAT IS NOT NULL) THEN S.STORE_FORMAT
           ELSE 0
       END AS LOC_FMT_CDE, --Q_STR_FT_SNUM
       CASE
           WHEN (SF.FORMAT_NAME IS NOT NULL) THEN SF.FORMAT_NAME
           ELSE 'N/A'
       END AS LOC_FMT_CDE_DESC,
CASE
    WHEN (UPPER(S.CHANNEL) LIKE 'WAREHOUSE') THEN 1
    WHEN (UPPER(S.CHANNEL) LIKE 'WEB') THEN 2
    WHEN (UPPER(S.CHANNEL) LIKE 'RETAIL') THEN 3
    WHEN (UPPER(S.CHANNEL) LIKE 'OUTLET') THEN 4
    ELSE -1
END AS CHNL_ID,
       CASE
           WHEN (S.CHANNEL IS NOT NULL) THEN S.CHANNEL
           ELSE '0'
       END AS CHNL_DESC, --Q_CHN_SNUM
       CASE
           WHEN (S.BRAND IS NOT NULL) THEN S.BRAND
           ELSE '0'
       END AS CHN_ID, --BRND_SNUM
       CASE
           WHEN (S.AREA IS NOT NULL) THEN S.AREA
           ELSE 'N/A'
       END AS ARA_ID, --AREA
       CASE
           WHEN (WH.WH IS NULL) THEN CASE
                                        WHEN (S.PHY_WH_IDNT IS NOT NULL) THEN CAST(S.PHY_WH_IDNT AS NUMBER)  
                                        ELSE '0'
                                    END
           ELSE WH.WH
       END AS LOC_PHY_WH_NUM,--Q_DC_SNUM
        -1 AS LOC_VIRTUAL_WH_NUM,
        -1 AS LOC_VAT_RGN,
        '-1' AS TSF_ZONE_CDE,
        '-1' AS TSF_ZONE_CDE_DESC,
       CASE
           WHEN (S.CURRENCY_CODE IS NOT NULL) THEN S.CURRENCY_CODE
           ELSE 'N/A'
       END AS CNCY_CDE, --RMS_Q_CCY_CD
       CASE
            WHEN (UPPER(S.LOC_TYPE) LIKE 'STORE') THEN -1
            WHEN (UPPER(S.LOC_TYPE) LIKE 'ONLINE') THEN -1
            ELSE -1
        END AS LOC_TYP_CDE,
       CASE
           WHEN (S.LOC_TYPE IS NOT NULL) THEN S.LOC_TYPE
           ELSE 'N/A'
       END AS LOC_TYP_CDE_DESC,
       CASE
           WHEN (S.STATE_CODE IS NOT NULL) THEN S.STATE_CODE
           ELSE 'N/A'
       END AS LOC_STATE_CDE,
       CASE
           WHEN (S.STATE_NAME IS NOT NULL) THEN S.STATE_NAME
           ELSE 'N/A'
       END AS LOC_STATE_CDE_DESC,
       '-1' AS STR_TYP_CDE,
       '-1' AS STR_TYP_CDE_DESC,
       CASE
           WHEN (S.COUNTRY_CODE IS NOT NULL) THEN S.COUNTRY_CODE
           ELSE 'N/A'
       END AS LOC_COUNTRY_CDE,
       CASE
           WHEN (S.COUNTRY IS NOT NULL) THEN S.COUNTRY
           ELSE 'N/A'
       END AS LOC_COUNTRY_CDE_DESC,
       -1 AS LOC_UPS_DST,
       CASE
           WHEN (S.STOCKHOLDING_IND IS NOT NULL) THEN S.STOCKHOLDING_IND
           ELSE 'N/A'
       END AS LOC_STKHLD_IND,
       '-1' AS LOC_VAT_INCLUDE_IND,
        '-1' AS LOC_BREAK_PACK_IND,
        '-1' AS LOC_DEFAULT_WH_NUM,
        '9999-12-31' AS LOC_LAST_REMODEL_DT,
/*Extra Columns Not in Mapping*/
       CASE
           WHEN (S.STORE_NO IS NOT NULL) THEN CAST(S.STORE_NO AS NUMBER)  
           ELSE 9999
       END AS RMS_Q_STR_DNUM, --(Mapping Q_DC_DNUM to Q_STR_DNUM)
       CASE
           WHEN (S.STORE_NAME IS NOT NULL) THEN S.STORE_NAME
           ELSE 'N/A'
       END AS RMS_Q_STR_SDESC,  --(Mapping Q_STR_SDESC to Q_DC_SDESC)
       CASE
           WHEN ('1900-01-01' IS NOT NULL) THEN '1900-01-01'
           ELSE '1900-01-01'
       END AS RMS_Q_STR_CMP_DT,
       CASE
           WHEN (S.SELLING_SQUARE_FT IS NOT NULL) THEN S.SELLING_SQUARE_FT
           ELSE 0
       END AS RMS_Q_STR_SLS_SQFT,
       CASE
           WHEN (CASE
                     WHEN (S.STOCKHOLDING_IND = 'Y') THEN 1
                     ELSE 0
                 END IS NOT NULL) THEN CASE
                                       WHEN (S.STOCKHOLDING_IND = 'Y') THEN 1
                                       ELSE '0'
                                   END
           ELSE '0'
       END AS RMS_Q_STR_SELLING_FLG,
       CASE
           WHEN (CASE
                     WHEN (WH.WH IS NULL) THEN 0
                     ELSE 1
                 END IS NOT NULL) THEN CASE
                                       WHEN (WH.WH IS NULL) THEN 0
                                       ELSE 1
                                   END
           WHEN (0 IS NOT NULL) THEN 0
           ELSE 0
       END AS RMS_Q_STR_DC_FLG,
       CASE
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           ELSE 'N/A'
       END AS RMS_Q_STR_GRD,
       CASE
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           ELSE 'N/A'
       END AS RMS_Q_STR_STATUS,
       CASE
           WHEN (S.ADDRESS IS NOT NULL) THEN S.ADDRESS
           ELSE 'N/A'
       END AS RMS_Q_STR_ADDR1,
       CASE
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           WHEN ('N/A' IS NOT NULL) THEN 'N/A'
           ELSE 'N/A'
       END AS RMS_Q_STR_ADDR2,
       CASE
           WHEN (S.CITY IS NOT NULL) THEN S.CITY
           ELSE 'N/A'
       END AS RMS_Q_STR_CITY,
       CASE
           WHEN (S.ZIPCODE IS NOT NULL) THEN S.ZIPCODE
           ELSE '0'
       END AS RMS_Q_STR_ZIP,
       CASE
           WHEN (S.LONGITUTE IS NOT NULL) THEN S.LONGITUTE
           ELSE '0'
       END AS RMS_Q_STR_LONG,
       CASE
           WHEN (S.LATITUTE IS NOT NULL) THEN S.LATITUTE
           ELSE '0'
       END AS RMS_Q_STR_LATI,
       CASE
           WHEN (S.CLIMATE IS NOT NULL) THEN S.CLIMATE
           ELSE 'N/A'
       END AS RMS_CLIMATE,
       CASE
           WHEN (S.TIER IS NOT NULL) THEN S.TIER
           ELSE 'N/A'
       END AS RMS_TIER,
       CASE
           WHEN (S.VOLUME IS NOT NULL) THEN S.VOLUME
           ELSE 'N/A'
       END AS RMS_VOLUME,
       CASE
           WHEN (S.TOTAL_SQUARE_FT IS NOT NULL) THEN S.TOTAL_SQUARE_FT
           ELSE '0'
       END AS RMS_TOTAL_SQUARE_FT,
        -1 AS RMS_ORG_HIER_TYPE,
        '-1' AS RMS_WH_ADD1,
        '-1' AS RMS_PROTECTED_IND,
        -1 AS RMS_Q_DC_ID,
       '0' AS RMS_SRC_ID,
       CURRENT_DATE AS RMS_LOAD_DATE--select *
FROM {{source('DW_LND_RMS','GYM_LOC_EXTRACT_QS_V')}} AS S
    LEFT JOIN {{source('DW_LND_RMS','STORE_FORMAT')}} AS SF ON S.STORE_FORMAT = SF.STORE_FORMAT /*For Store_Format and its Name*/
      LEFT JOIN {{source('DW_LND_RMS','WH')}} AS WH ON S.STORE_NO = WH.WH AND WH.WH_NAME <> 'zzz'
        LEFT JOIN {{source('DW_LND_RMS','BRAND')}} AS B ON B.BRAND = S.BRAND
        WHERE
        B.BRAND_DESC IN ('NexusNova', 'DC')
        AND S.STORE_NAME <> 'zzz'
UNION
SELECT
       CASE
           WHEN (S.WH IS NOT NULL) THEN CAST(S.WH AS NUMBER)
           ELSE 0
       END AS LOC_ID, --Q_DC_SNUM
       CASE
           WHEN (S.WH_NAME IS NOT NULL) THEN S.WH_NAME
           ELSE 'N/A'
       END AS LOC_DESC, --Q_DC_DESC
        '0000-01-01' AS LOC_START_DT,
        '9999-12-31' AS LOC_END_DT,
        '-1' AS DST_ID,
        -1 AS LOC_FMT_CDE,
        'N/A' AS LOC_FMT_CDE_DESC,
       CASE
           WHEN (S.CHANNEL_ID IS NOT NULL) THEN S.CHANNEL_ID
           ELSE 0
       END AS CHNL_ID,  --CHANNEL_ID
CASE
    WHEN S.CHANNEL_ID = '1' THEN 'Warehouse'
    WHEN S.CHANNEL_ID = '2' THEN 'Web'
    WHEN S.CHANNEL_ID = '3' THEN 'Retail'
    WHEN S.CHANNEL_ID = '4' THEN 'Outlet'
    ELSE '-1'
END AS CHNL_DESC,   --select * from dw_lnd_rms.wh;
        '-1' AS CHN_ID,
        '-1' AS ARA_ID,
       CASE
           WHEN (S.PHYSICAL_WH IS NOT NULL) THEN S.PHYSICAL_WH
           ELSE 0
       END AS LOC_PHY_WH_NUM, --PHYSICAL_WH
       CASE
           WHEN (S.PRIMARY_VWH IS NOT NULL) THEN S.PRIMARY_VWH
           ELSE 0
       END AS LOC_VIRTUAL_WH_NUM,  --PRIMARY_VWH
       CASE
           WHEN (S.VAT_REGION IS NOT NULL) THEN S.VAT_REGION
           ELSE 0
       END AS LOC_VAT_RGN, --VAT_REGION
       CASE
           WHEN ('0' IS NOT NULL) THEN '0'
           WHEN ('0' IS NOT NULL) THEN '0'
           ELSE '0'
       END AS TSF_ZONE_CDE,  --TSF_ENTITY_ID
       '-1' AS TSF_ZONE_CDE_DESC,
       CASE
           WHEN (S.CURRENCY_CODE IS NOT NULL) THEN S.CURRENCY_CODE
           ELSE 'N/A'
       END AS CNCY_CDE, --CURRENCY_CODE
       -1 AS LOC_TYP_CDE,
       'WH' AS LOC_TYP_CDE_DESC,
        '-1' AS LOC_STATE_CDE,
        '-1' AS LOC_STATE_CDE_DESC,
        '-1' AS STR_TYP_CDE,
       '-1' AS STR_TYP_CDE_DESC,
       '-1' AS LOC_COUNTRY_CDE,
       '-1' AS LOC_COUNTRY_CDE_DESC,
       -1 AS LOC_UPS_DST,
       CASE
           WHEN (S.STOCKHOLDING_IND IS NOT NULL) THEN S.STOCKHOLDING_IND
           ELSE 'N/A'
       END AS LOC_STKHLD_IND,
       '-1' AS LOC_VAT_INCLUDE_IND,
       '-1' AS LOC_BREAK_PACK_IND,
       '-1' AS LOC_DEFAULT_WH_NUM,
       '9999-12-31' AS LOC_LAST_REMODEL_DT,
/*Extra Columns in Mapping*/
       CASE
           WHEN (S.WH IS NOT NULL) THEN S.WH
           ELSE 9999
       END AS RMS_Q_STR_DNUM, --(Mapping Q_DC_DNUM to Q_STR_DNUM)
       CASE
           WHEN (S.WH_NAME IS NOT NULL) THEN S.WH_NAME
           ELSE 'N/A'
       END AS RMS_Q_STR_DESC,  --(Mapping Q_DC_SDESC to Q_STR_SDESC)
        '-1' AS RMS_Q_STR_CMP_DT,
        -1 AS RMS_Q_STR_SLS_SQFT,
        '-1' AS RMS_Q_STR_SELLING_FLG,
        -1 AS RMS_Q_STR_DC_FLG,
        '-1' AS RMS_Q_STR_GRD,
        '-1' AS RMS_Q_STR_STATUS,
        '-1' AS RMS_Q_STR_ADDR1,
        '-1' AS RMS_Q_STR_ADDR2,
        '-1' AS RMS_Q_STR_CITY,
        '-1' AS RMS_Q_STR_ZIP,
        '-1' AS RMS_Q_STR_LONG,
        '-1' AS RMS_Q_STR_LATI,
        '-1' AS RMS_CLIMATE,
        '-1' AS RMS_TIER,
        '-1' AS RMS_VOLUME,
        '-1' AS RMS_TOTAL_SQUARE_FT,
       CASE
           WHEN (S.ORG_HIER_TYPE IS NOT NULL) THEN S.ORG_HIER_TYPE
           ELSE 0
       END AS RMS_ORG_HIER_TYPE,
       CASE
           WHEN (S.WH_ADD1 IS NOT NULL) THEN S.WH_ADD1
           ELSE 'N/A'
       END AS RMS_WH_ADD1,
       CASE
           WHEN (S.PROTECTED_IND IS NOT NULL) THEN S.PROTECTED_IND
           ELSE 'N/A'
       END AS RMS_PROTECTED_IND,
        T.Q_STR_ID AS RMS_Q_DC_ID,
       '0' AS RMS_SRC_ID,
       CURRENT_DATE AS RMS_LOAD_DATE--select *
FROM {{source('DW_LND_RMS','WH')}} AS S
      LEFT JOIN {{source('DW_LND_RMS','DW_LOC_STR')}} AS T ON ((CASE
                                       WHEN (S.WH IS NOT NULL) THEN S.WH
                                       ELSE '0'
                                   END = T.Q_DC_ID))
)

select * from V_STG_D_ORG_LOC_LU 