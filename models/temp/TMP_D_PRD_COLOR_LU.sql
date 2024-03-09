
{{config(
    tags=["color"]
    ,pre_hook=mpre_hook(c_package="COLOR", c_status="RUNNING", c_bookmark="AFTER_STG")
    )}}

SELECT 
    COLOR_ID,
    COLOR_DESC,
    COLOR_CIN_CDE,
    COLOR_FAMILY_DESC,
    RMS_Q_CLR_DNUM,
    RMS_DIFF_TYPE,
    RMS_SRC_ID
FROM {{ref('V_STG_D_PRD_COLOR_LU')}}