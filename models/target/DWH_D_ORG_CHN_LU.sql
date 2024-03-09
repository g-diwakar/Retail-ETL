{{config(
    unique_key='CHN_ID', 
    tags=["chn"]
    ,pre_hook=mpre_hook(c_package="CHN", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with 

using_clause as 
(
    select
        {{dbt_utils.generate_surrogate_key(['chn_id','chn_desc'])}} as CHN_KEY, 
        chn_id,
        chn_desc,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        CHN_MGR_FIRST_NAME,
        CHN_MGR_LAST_NAME,
        CHN_MGR_HOME_STR,
        CHN_MGR_PHONE,
        CHN_MGR_MAIL_EXT,
        CHN_MGR_COUNTRY_CDE,
        CHN_MGR_ASSISTANT_NAME,
        CHN_MGR_MOBILE,
        RMS_SRC_ID,
        RMS_BRND_SDESC,
        RMS_BRND_DNUM

    from {{ref('TMP_D_ORG_CHN_LU')}}
),

updates as 
(
    select 
        CHN_KEY,
        chn_id,
        chn_desc,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        CHN_MGR_FIRST_NAME,
        CHN_MGR_LAST_NAME,
        CHN_MGR_HOME_STR,
        CHN_MGR_PHONE,
        CHN_MGR_MAIL_EXT,
        CHN_MGR_COUNTRY_CDE,
        CHN_MGR_ASSISTANT_NAME,
        CHN_MGR_MOBILE,
        RMS_SRC_ID,
        RMS_BRND_SDESC,
        RMS_BRND_DNUM,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

    from using_clause 

    {% if is_incremental() %}
    where chn_id in (select chn_id from {{this}})
    {% endif %}

),

inserts as
(
    select 
        CHN_KEY,
        chn_id,
        chn_desc,
        COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        CHN_MGR_FIRST_NAME,
        CHN_MGR_LAST_NAME,
        CHN_MGR_HOME_STR,
        CHN_MGR_PHONE,
        CHN_MGR_MAIL_EXT,
        CHN_MGR_COUNTRY_CDE,
        CHN_MGR_ASSISTANT_NAME,
        CHN_MGR_MOBILE,
        RMS_SRC_ID,
        RMS_BRND_SDESC,
        RMS_BRND_DNUM,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT

    from using_clause
    where chn_id not in (select chn_id from updates)
)

select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
