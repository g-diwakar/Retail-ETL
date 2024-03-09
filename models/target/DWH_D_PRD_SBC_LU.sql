{{config(
    unique_key = ['SBC_ID','CLS_ID','DPT_ID'], 
    tags=["sbc"]
    ,pre_hook=mpre_hook(c_package="SBC", c_status="RUNNING", c_bookmark="AFTER_TMP")
    )}}

with using_clause as 
(
    SELECT 
        {{dbt_utils.generate_surrogate_key(['SBC_ID','CLS_ID','DPT_ID','SBC_DESC'])}} as SBC_KEY,
        SBC_ID,
        SBC_DESC,
        CLS_KEY,
        CLS_ID,
        CLS_DESC,
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
        RMS_SBCLASS_MRCH_IDNT,
        RMS_SBCLASS_BUYR_IDNT,
        RMS_Q_IMG_LG,
        RMS_LOAD_DATE,
        RMS_Q_IMG_DFLT,
        RMS_SBCLASS_MRCH_NAME,
        RMS_SBCLASS_BUYR_NAME,
        RMS_Q_IMG_SM,
        RMS_Q_DPT_SNUM,
        RMS_Q_DPT_DNUM,
        RMS_SRC_ID,
        RMS_Q_SBC_SDESC

    FROM {{ref('TMP_D_PRD_SBC_LU')}}
),

updates as
(
    SELECT 
        SBC_KEY,
        SBC_ID,
        SBC_DESC,
        CLS_KEY,
        CLS_ID,
        CLS_DESC,
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
        RMS_SBCLASS_MRCH_IDNT,
        RMS_SBCLASS_BUYR_IDNT,
        RMS_Q_IMG_LG,
        RMS_LOAD_DATE,
        RMS_Q_IMG_DFLT,
        RMS_SBCLASS_MRCH_NAME,
        RMS_SBCLASS_BUYR_NAME,
        RMS_Q_IMG_SM,
        RMS_Q_DPT_SNUM,
        RMS_Q_DPT_DNUM,
        RMS_SRC_ID,
        RMS_Q_SBC_SDESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}
    WHERE SBC_ID in (SELECT SBC_ID from {{this}})
    AND
    CLS_ID in (SELECT CLS_ID from {{this}})
    AND
    DPT_ID in (SELECT DPT_ID from {{this}})

    {% endif %}
),

inserts as
(
   SELECT 
        SBC_KEY,
        SBC_ID,
        SBC_DESC,
        CLS_KEY,
        CLS_ID,
        CLS_DESC,
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
        RMS_SBCLASS_MRCH_IDNT,
        RMS_SBCLASS_BUYR_IDNT,
        RMS_Q_IMG_LG,
        RMS_LOAD_DATE,
        RMS_Q_IMG_DFLT,
        RMS_SBCLASS_MRCH_NAME,
        RMS_SBCLASS_BUYR_NAME,
        RMS_Q_IMG_SM,
        RMS_Q_DPT_SNUM,
        RMS_Q_DPT_DNUM,
        RMS_SRC_ID,
        RMS_Q_SBC_SDESC,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause  

    WHERE SBC_ID not in (SELECT SBC_ID from updates)
    AND
    CLS_ID not in (SELECT CLS_ID from updates)
    AND
    DPT_ID not in (SELECT DPT_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 

