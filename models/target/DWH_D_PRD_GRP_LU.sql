{{config(
    unique_key='GRP_ID', 
    tags=["grp"]
    ,pre_hook=mpre_hook(c_package="GRP", c_status="RUNNING", c_bookmark="AFTER_TMP")
)}}

with using_clause as
(
    SELECT 
        {{dbt_utils.generate_surrogate_key(['GRP_ID','GRP_DESC'])}} as GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_Q_BYR_DNUM,
        RMS_Q_BYR_SDESC,
        RMS_GRP_BUYR_NAME,
        RMS_SRC_ID,
        RMS_GRP_MRCH_NAME,
        RMS_GRP_MRCH_IDNT,
        RMS_GRP_BUYR_IDNT,
        RMS_LOAD_DATE

    FROM {{ref('TMP_D_PRD_GRP_LU')}}
),

updates as 
(
    SELECT 
        GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_Q_BYR_DNUM,
        RMS_Q_BYR_SDESC,
        RMS_GRP_BUYR_NAME,
        RMS_SRC_ID,
        RMS_GRP_MRCH_NAME,
        RMS_GRP_MRCH_IDNT,
        RMS_GRP_BUYR_IDNT,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        FROM using_clause

        {% if is_incremental() %}
        WHERE GRP_ID in (SELECT GRP_ID from {{this}})
        {% endif %}

),

inserts as 
(
    SELECT 
        GRP_KEY,
        GRP_ID,
        GRP_DESC,
        DIV_KEY,
        DIV_ID,
        DIV_DESC,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        RMS_Q_BYR_DNUM,
        RMS_Q_BYR_SDESC,
        RMS_GRP_BUYR_NAME,
        RMS_SRC_ID,
        RMS_GRP_MRCH_NAME,
        RMS_GRP_MRCH_IDNT,
        RMS_GRP_BUYR_IDNT,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        FROM using_clause

        where GRP_ID not in (SELECT GRP_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
