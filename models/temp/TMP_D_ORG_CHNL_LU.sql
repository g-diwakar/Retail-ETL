
{{config(
    tags=["chnl"]
   	,pre_hook=mpre_hook(c_package="CHNL", c_status="RUNNING", c_bookmark="AFTER_STG") 
)}}

SELECT 
    CHNL_ID,
    CHNL_DESC
FROM {{ref('V_STG_D_ORG_CHNL_LU')}}