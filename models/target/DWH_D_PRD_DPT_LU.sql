{{config(
    unique_key = 'DPT_ID', 
    tags=["dpt"]
    ,pre_hook=mpre_hook(c_package="DPT", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with using_clause as
(
    SELECT 
        {{dbt_utils.generate_surrogate_key(['DPT_ID','DPT_DESC'])}} as DPT_KEY,
        DPT_ID,
        DPT_DESC,
        GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_BUD_MKUP,
        RMS_PRFT_CALC_TYPE_CDE,
        RMS_PURCH_TYPE_CDE,
        RMS_LOAD_DATE,
        RMS_SRC_ID,
        RMS_OTB_CALC_TYPE_CDE,
        RMS_Q_DPT_BYR_NM,
        RMS_Q_DPT_SDESC,
        RMS_MKUP_CALC_TYPE_CDE,
        RMS_Q_DPT_DNUM,
        RMS_BUD_INT

    FROM {{ref('TMP_D_PRD_DPT_LU')}}
),

updates as
(
    SELECT
        DPT_KEY,
        DPT_ID,
        DPT_DESC,
        GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_BUD_MKUP,
        RMS_PRFT_CALC_TYPE_CDE,
        RMS_PURCH_TYPE_CDE,
        RMS_LOAD_DATE,
        RMS_SRC_ID,
        RMS_OTB_CALC_TYPE_CDE,
        RMS_Q_DPT_BYR_NM,
        RMS_Q_DPT_SDESC,
        RMS_MKUP_CALC_TYPE_CDE,
        RMS_Q_DPT_DNUM,
        RMS_BUD_INT,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        FROM using_clause

        {% if is_incremental() %}

        where DPT_ID in (SELECT DPT_ID from {{this}})

        {% endif %}
),

inserts as
(
    SELECT
        DPT_KEY,
        DPT_ID,
        DPT_DESC,
        GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_BUD_MKUP,
        RMS_PRFT_CALC_TYPE_CDE,
        RMS_PURCH_TYPE_CDE,
        RMS_LOAD_DATE,
        RMS_SRC_ID,
        RMS_OTB_CALC_TYPE_CDE,
        RMS_Q_DPT_BYR_NM,
        RMS_Q_DPT_SDESC,
        RMS_MKUP_CALC_TYPE_CDE,
        RMS_Q_DPT_DNUM,
        RMS_BUD_INT,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

    FROM using_clause

    WHERE DPT_ID not in (SELECT DPT_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
