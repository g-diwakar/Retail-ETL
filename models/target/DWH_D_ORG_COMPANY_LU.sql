{{config(
    unique_key='COMPANY_ID',
    tags=["company"]
    ,pre_hook=mpre_hook(c_package="COMPANY", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}



with 

using_clause as
(
    select 
        COMPANY_ID,
        COMPANY_DESC,
    from {{ref('TMP_D_ORG_COMPANY_LU')}}
),

updates as
(
    select 
        {{dbt_utils.generate_surrogate_key(['company_id','COMPANY_DESC'])}} as COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
        from using_clause

        {% if is_incremental() %}
            where COMPANY_ID in (select COMPANY_ID from {{this}})
        {% endif %}
),

inserts as
(
    select 
       {{dbt_utils.generate_surrogate_key(['company_id','COMPANY_DESC'])}} as COMPANY_KEY,
        COMPANY_ID,
        COMPANY_DESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG ,
        '9999-12-31' as RCD_CLOSE_DT
        from using_clause

        where COMPANY_ID not in (select COMPANY_ID from updates)
)

select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 