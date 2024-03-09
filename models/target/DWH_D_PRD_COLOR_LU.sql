{{config(
    unique_key = 'COLOR_ID', 
    tags=["color"]
    ,pre_hook=mpre_hook(c_package="COLOR", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with 

using_clause as
(
    SELECT 
        {{dbt_utils.generate_surrogate_key(['COLOR_ID','COLOR_DESC'])}} as COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        COLOR_CIN_CDE,
        COLOR_FAMILY_DESC,
        RMS_Q_CLR_DNUM,
        RMS_DIFF_TYPE,
        RMS_SRC_ID
    FROM {{ref('V_STG_D_PRD_COLOR_LU')}}
),

updates as
(
    SELECT 
        COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        COLOR_CIN_CDE,
        COLOR_FAMILY_DESC,
        RMS_Q_CLR_DNUM,
        RMS_DIFF_TYPE,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}

    WHERE COLOR_ID in (SELECT COLOR_ID from {{this}})

    {% endif %}
 
),

inserts as
(
    SELECT 
        COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        COLOR_CIN_CDE,
        COLOR_FAMILY_DESC,
        RMS_Q_CLR_DNUM,
        RMS_DIFF_TYPE,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    WHERE COLOR_ID not in (SELECT COLOR_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
