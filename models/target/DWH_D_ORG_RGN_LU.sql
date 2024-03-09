{{config(
    unique_key='RGN_ID', 
    tags=["rgn"]
    ,pre_hook=mpre_hook(c_package="RGN", c_status="RUNNING", c_bookmark="AFTER_TMP")
)}}

with 
using_clause as
(
    SELECT

    {{dbt_utils.generate_surrogate_key(['RGN_ID','RGN_DESC'])}} as RGN_KEY,
    RGN_ID,
    RGN_DESC,
    ARA_KEY,
    ARA_ID,
    ARA_DESC,
    CHN_KEY,
    CHN_ID,
    CHN_DESC,
    COMPANY_KEY,
    COMPANY_ID,
    COMPANY_DESC,
    RGN_MGR_FIRST_NAME,
    RGN_MGR_LAST_NAME,
    RGN_MGR_HOME_STR,
    RGN_MGR_PHONE,
    RGN_MGR_MAIL_EXT,
    RGN_MGR_ASSISTANT_NAME,
    RGN_MGR_MOBILE,
    RMS_Q_RGN_SDESC,
    RMS_SRC_ID,
    RMS_Q_RGN_DNUM,
    RMS_LOAD_DATE

    FROM {{ref('TMP_D_ORG_RGN_LU')}}
),

updates as
(
    SELECT
        RGN_KEY,
        RGN_ID,
        RGN_DESC,
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        COMPANY_KEY,
        COMPANY_DESC,
        RGN_MGR_FIRST_NAME,
        RGN_MGR_LAST_NAME,
        RGN_MGR_HOME_STR,
        RGN_MGR_PHONE,
        RGN_MGR_MAIL_EXT,
        RGN_MGR_ASSISTANT_NAME,
        RGN_MGR_MOBILE,
        RMS_Q_RGN_SDESC,
        RMS_SRC_ID,
        RMS_Q_RGN_DNUM,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}
    where RGN_ID in (select RGN_ID from {{this}})
    {% endif %}
),

inserts as
(
    SELECT
        RGN_KEY,
        RGN_ID,
        RGN_DESC,
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        COMPANY_KEY,
        COMPANY_DESC,
        RGN_MGR_FIRST_NAME,
        RGN_MGR_LAST_NAME,
        RGN_MGR_HOME_STR,
        RGN_MGR_PHONE,
        RGN_MGR_MAIL_EXT,
        RGN_MGR_ASSISTANT_NAME,
        RGN_MGR_MOBILE,
        RMS_Q_RGN_SDESC,
        RMS_SRC_ID,
        RMS_Q_RGN_DNUM,
        RMS_LOAD_DATE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    where RGN_ID not in (SELECT RGN_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 

