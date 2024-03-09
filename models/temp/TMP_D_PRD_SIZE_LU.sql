{{config(
    tags=["size"]
    ,pre_hook=mpre_hook(c_package="SIZE", c_status="RUNNING", c_bookmark="AFTER_STG")
    )}}

SELECT
    SIZE_ID,
    SIZE_DESC,
    INDUSTRY_CDE,
    INDUSTRY_SUBGRP,
    RMS_DIFF_TYPE,
    RMS_SRC_ID
FROM {{ref('V_STG_D_PRD_SIZE_LU')}}