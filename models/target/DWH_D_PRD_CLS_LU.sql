{{config(
    unique_key = 'CLS_ID', 
    tags=["cls"]
    ,pre_hook=mpre_hook(c_package="CLS", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with 

using_clause as
(
    SELECT
        {{dbt_utils.generate_surrogate_key(['CLS_ID','CLS_DESC'])}} as CLS_KEY,
        CLS_ID,
        DPT_ID,
        CLS_DESC,
        DPT_KEY,
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
        RMS_CLASS_MRCH_NAME,
        RMS_Q_CLS_SDESC,
        RMS_LOAD_DATE,
        RMS_CLASS_MRCH_IDNT,
        RMS_SRC_ID,
        RMS_CLASS_BUYR_SNUM,
        RMS_Q_CLS_DNUM,
        RMS_CLASS_BUYR_NAME

    FROM {{ref('TMP_D_PRD_CLS_LU')}}
),

updates as
(
    SELECT 
        CLS_KEY,
        CLS_ID,
        DPT_ID,
        CLS_DESC,
        DPT_KEY,
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
        RMS_CLASS_MRCH_NAME,
        RMS_Q_CLS_SDESC,
        RMS_LOAD_DATE,
        RMS_CLASS_MRCH_IDNT,
        RMS_SRC_ID,
        RMS_CLASS_BUYR_SNUM,
        RMS_Q_CLS_DNUM,
        RMS_CLASS_BUYR_NAME,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}
    WHERE CLS_ID in (SELECT CLS_ID from {{this}})
    {% endif %}
),

inserts as 
(
    SELECT 
        CLS_KEY,
        CLS_ID,
        DPT_ID,
        CLS_DESC,
        DPT_KEY,
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
        RMS_CLASS_MRCH_NAME,
        RMS_Q_CLS_SDESC,
        RMS_LOAD_DATE,
        RMS_CLASS_MRCH_IDNT,
        RMS_SRC_ID,
        RMS_CLASS_BUYR_SNUM,
        RMS_Q_CLS_DNUM,
        RMS_CLASS_BUYR_NAME,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    WHERE CLS_ID not in (SELECT CLS_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 

