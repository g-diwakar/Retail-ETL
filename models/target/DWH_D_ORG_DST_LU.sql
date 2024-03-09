{{config(
    unique_key='DST_ID', 
    tags=["dst"]
    ,pre_hook=mpre_hook(c_package="DST", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with 

using_clause as
(
    SELECT 
        {{dbt_utils.generate_surrogate_key(['DST_ID','DST_DESC'])}} as DST_KEY,
        DST_ID,
        DST_DESC,
        RGN_KEY,
        RGN_ID,
        RGN_DESC,
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        --COMPANY_KEY,
        --COMPANY_ID,
        --COMPANY_DESC,
        DST_MGR_FIRST_NAME,
        DST_MGR_LAST_NAME,
        DST_MGR_HOME_STR,
        DST_MGR_PHONE,
        DST_MGR_MAIL_EXT,
        DST_MGR_COUNTRY_CDE,
        DST_MGR_ASSISTANT_NAME,
        DST_MGR_MOBILE,
        RMS_Q_DST_SDESC,
        RMS_LOAD_DATE,
        RMS_Q_DST_DNUM,
        RMS_SRC_ID
    FROM {{ref('TMP_D_ORG_DST_LU')}}
),

updates as
(
    SELECT 
        DST_KEY,
        DST_ID,
        DST_DESC,
        RGN_KEY,
        RGN_ID,
        RGN_DESC,
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        --COMPANY_KEY,
        --COMPANY_ID,
        --COMPANY_DESC,
        DST_MGR_FIRST_NAME,
        DST_MGR_LAST_NAME,
        DST_MGR_HOME_STR,
        DST_MGR_PHONE,
        DST_MGR_MAIL_EXT,
        DST_MGR_COUNTRY_CDE,
        DST_MGR_ASSISTANT_NAME,
        DST_MGR_MOBILE,
        RMS_Q_DST_SDESC,
        RMS_LOAD_DATE,
        RMS_Q_DST_DNUM,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

        from using_clause

        {% if is_incremental() %}

        where DST_ID in (select DST_ID from {{this}})

        {% endif %}
 
),

inserts as
(
    SELECT
       DST_KEY,
        DST_ID,
        DST_DESC,
        RGN_KEY,
        RGN_ID,
        RGN_DESC,
        ARA_KEY,
        ARA_ID,
        ARA_DESC,
        CHN_KEY,
        CHN_ID,
        CHN_DESC,
        --COMPANY_KEY,
        --COMPANY_ID,
        --COMPANY_DESC,
        DST_MGR_FIRST_NAME,
        DST_MGR_LAST_NAME,
        DST_MGR_HOME_STR,
        DST_MGR_PHONE,
        DST_MGR_MAIL_EXT,
        DST_MGR_COUNTRY_CDE,
        DST_MGR_ASSISTANT_NAME,
        DST_MGR_MOBILE,
        RMS_Q_DST_SDESC,
        RMS_LOAD_DATE,
        RMS_Q_DST_DNUM,
        RMS_SRC_ID,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

    FROM using_clause

    where DST_ID not in (select DST_ID from updates)

)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
