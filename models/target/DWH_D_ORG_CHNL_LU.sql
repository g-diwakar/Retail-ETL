{{config(
        unique_key='CHNL_ID',
        tags=["chnl"]
	    ,pre_hook=mpre_hook(c_package="CHNL", c_status="RUNNING", c_bookmark="AFTER_TMP")
)}}

with 

using_clause as
(
    SELECT
        {{dbt_utils.generate_surrogate_key(['CHNL_ID','CHNL_DESC'])}} as CHNL_KEY,
        CHNL_ID,
        CHNL_DESC
    FROM {{ref('TMP_D_ORG_CHNL_LU')}}
),

updates as
(
    SELECT 
        CHNL_KEY,
        CHNL_ID,
        CHNL_DESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
 
    FROM using_clause 

    {% if is_incremental() %}
    where CHNL_ID in (SELECT CHNL_ID from {{this}})
    {% endif %}
),

inserts as
(
    SELECT 
        CHNL_KEY,
        CHNL_ID,
        CHNL_DESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
 
    FROM using_clause

    where CHNL_ID not in (SELECT CHNL_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
