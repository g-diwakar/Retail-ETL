{{config(
    unique_key='ARA_ID', 
    tags=["ara"]
    ,pre_hook=mpre_hook(c_package="ARA", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with 
using_clause as
(
    select
        {{dbt_utils.generate_surrogate_key(['ARA_ID','ARA_DESC'])}} as ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        COMPANY_KEY,
        COMPANY_DESC,
        COMPANY_ID,
        ARA_MGR_FIRST_NAME,
        ARA_MGR_LAST_NAME,
        ARA_MGR_HOME_STR,
        ARA_MGR_MAIL_EXT,
        ARA_MGR_COUNTRY_CDE,
        ARA_MGR_ASSISTANT_NAME,
        ARA_MGR_MOBILE,
        RMS_Q_CPY_DNUM,
        RMS_Q_CPY_SDESC
    FROM {{ref('TMP_D_ORG_ARA_LU')}}
),

updates as
(
    SELECT 
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        COMPANY_KEY,
        COMPANY_DESC,
        COMPANY_ID,
        ARA_MGR_FIRST_NAME,
        ARA_MGR_LAST_NAME,
        ARA_MGR_HOME_STR,
        ARA_MGR_MAIL_EXT,
        ARA_MGR_COUNTRY_CDE,
        ARA_MGR_ASSISTANT_NAME,
        ARA_MGR_MOBILE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT,
        RMS_Q_CPY_DNUM,
        RMS_Q_CPY_SDESC,

    FROM using_clause

    {% if is_incremental() %}
    WHERE ARA_ID in (select ARA_ID from {{this}})
    {% endif %}
),

inserts as
(
    SELECT 
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        COMPANY_KEY,
        COMPANY_DESC,
        COMPANY_ID,
        ARA_MGR_FIRST_NAME,
        ARA_MGR_LAST_NAME,
        ARA_MGR_HOME_STR,
        ARA_MGR_MAIL_EXT,
        ARA_MGR_COUNTRY_CDE,
        ARA_MGR_ASSISTANT_NAME,
        ARA_MGR_MOBILE,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT,
        RMS_Q_CPY_DNUM,
        RMS_Q_CPY_SDESC,

    FROM using_clause

    where ARA_ID not in (select ARA_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
