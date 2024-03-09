{{config(
    unique_key='DIV_ID', 
    tags=["div"]
    ,pre_hook=mpre_hook(c_package="DIV", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with using_clause as
(
    SELECT
        {{dbt_utils.generate_surrogate_key(['DIV_ID','DIV_DESC'])}} as DIV_KEY,
        DIV_ID, 
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_DIV_MRCH_NAME,
        RMS_DIV_BYR_NAME,
        RMS_SRC_ID,
        RMS_Q_DMM_DNUM,
        RMS_DIV_MRCH_IDNT,
        RMS_DIV_BYR_IDNT,
        RMS_Q_DMM_SDESC,
        RMS_LOAD_DATE
    FROM {{ref('TMP_D_PRD_DIV_LU')}}
),

updates as
(
    SELECT
        DIV_KEY,
        DIV_ID, 
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_DIV_MRCH_NAME,
        RMS_DIV_BYR_NAME,
        RMS_SRC_ID,
        RMS_Q_DMM_DNUM,       
        RMS_DIV_MRCH_IDNT,
        RMS_DIV_BYR_IDNT,
        RMS_Q_DMM_SDESC,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        FROM using_clause

        {% if is_incremental() %}
        WHERE DIV_ID in (SELECT DIV_ID from {{this}})
        {% endif %}

),

inserts as
(
    SELECT 
        DIV_KEY,
        DIV_ID, 
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_DIV_MRCH_NAME,
        RMS_DIV_BYR_NAME,
        RMS_SRC_ID,
        RMS_Q_DMM_DNUM,       
        RMS_DIV_MRCH_IDNT,
        RMS_DIV_BYR_IDNT,
        RMS_Q_DMM_SDESC,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        FROM using_clause

        where DIV_ID not in (select DIV_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
