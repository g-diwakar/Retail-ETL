{{config(
    tags=["company"] 
    ,pre_hook=mpre_hook(c_package="COMPANY", c_status="RUNNING", c_bookmark="NONE")
)}}

with V_STG_D_ORG_COMPANY_LU as
(
SELECT COMPANY as COMPANY_ID, CO_NAME as COMPANY_DESC 
from {{source('DW_LND_RMS','COMPANY')}}
)

select * from V_STG_D_ORG_COMPANY_LU