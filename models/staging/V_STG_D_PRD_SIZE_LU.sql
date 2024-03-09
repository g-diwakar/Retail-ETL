{{config(
	tags=["size"]
   ,pre_hook=mpre_hook(c_package="SIZE", c_status="RUNNING", c_bookmark="NONE")
	)}}

with V_STG_D_PRD_SIZE_LU as 
(
SELECT NVL(DIF.DIFF_IDNT, '0') AS SIZE_ID
	,REGEXP_REPLACE(NVL(DIF.DIFF_DESC, 'N/A'), '\\s+', ' ') AS SIZE_DESC
	,NVL(DIF.INDUSTRY_CDE, 0) AS INDUSTRY_CDE
	,NVL(DIF.INDUSTRY_SUBGROUP, 'N/A') AS INDUSTRY_SUBGRP 
    /*FLEX-FILEDS*/
	,NVL(DIF.DIFF_TYPE, 'N/A') AS RMS_DIFF_TYPE
	,'0' AS RMS_SRC_ID 
    FROM {{source('DW_LND_RMS','PRDDIFFDM')}} AS DIF 
    WHERE DIF.DIFF_TYPE = 'S'

)

select * from V_STG_D_PRD_SIZE_LU 