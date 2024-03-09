{{config(
    unique_key='SIZE_ID', 
    tags=["size"]
    ,pre_hook=mpre_hook(c_package="SIZE", c_status="RUNNING", c_bookmark="AFTER_TMP")
)}}

with using_clause as
(
    SELECT
        {{dbt_utils.generate_surrogate_key(['SIZE_ID','SIZE_DESC'])}} as SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        INDUSTRY_CDE,
        INDUSTRY_SUBGRP,
        RMS_DIFF_TYPE,
        RMS_SRC_ID
    FROM {{ref('TMP_D_PRD_SIZE_LU')}}
),

updates as
(
    SELECT
        SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        INDUSTRY_CDE,
        INDUSTRY_SUBGRP,
        RMS_DIFF_TYPE,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}
    WHERE SIZE_ID in (SELECT SIZE_ID from {{this}})
    {% endif %}
),

inserts as 
(
    SELECT
        SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        INDUSTRY_CDE,
        INDUSTRY_SUBGRP,
        RMS_DIFF_TYPE,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    WHERE SIZE_ID not in (SELECT SIZE_ID from updates)

)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
