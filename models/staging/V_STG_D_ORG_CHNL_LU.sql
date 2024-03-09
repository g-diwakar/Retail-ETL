{{config(
    tags=["chnl"]
   	,pre_hook=mpre_hook(c_package="CHNL", c_status="RUNNING", c_bookmark="NONE") 
    )
    }}
with V_STG_D_ORG_CHNL_LU as 
(
    SELECT
        CASE 
            WHEN (UPPER(S.CHANNEL) LIKE 'WAREHOUSE') THEN 1
            WHEN (UPPER(S.CHANNEL) LIKE 'WEB') THEN 2
            WHEN (UPPER(S.CHANNEL) LIKE 'RETAIL') THEN 3
            WHEN (UPPER(S.CHANNEL) LIKE 'OUTLET') THEN 4
            ELSE -1
        END AS CHNL_ID,

        CASE 
            WHEN (S.CHANNEL) IS NOT NULL THEN S.CHANNEL
            ELSE '0'
        END AS CHNL_DESC
    
    FROM {{source('DW_LND_RMS','GYM_LOC_EXTRACT_QS_V')}} as S
)


SELECT * from V_STG_D_ORG_CHNL_LU
