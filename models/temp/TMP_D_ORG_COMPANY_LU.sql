{{config(
    tags=["company"]
    ,pre_hook=mpre_hook(c_package="COMPANY", c_status="RUNNING", c_bookmark="AFTER_STG")
)}}

SELECT COMPANY_ID, COMPANY_DESC 
FROM
{{ref('V_STG_D_ORG_COMPANY_LU')}}