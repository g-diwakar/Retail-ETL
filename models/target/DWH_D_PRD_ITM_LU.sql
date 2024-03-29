{{config(
    unique_key = 'ITM_ID', 
    tags=["itm"]
    ,pre_hook=mpre_hook(c_package="ITM", c_status="RUNNING", c_bookmark="AFTER_TMP")
)}}

with 
using_clause as
(
    SELECT
        {{dbt_utils.generate_surrogate_key(['ITM_ID','ITM_DESC'])}} as ITM_KEY,
        ITM_ID,
        ITM_DESC,
        STY_KEY,
        STY_ID,
        STY_DESC,
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
        UPC_ID,
        --SUP_KEY,
        --SUP_ID,
        --SUP_PART_ID,
        --SUP_DESC,
        COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        MER_IND,
        FCST_IND,
        INV_IND,
        PACK_IND,
        ORDERABLE_IND,
        SELLABLE_IND,
        SIMPLE_PACK_IND,
        STND_UOM_CDE,
        FIRST_RCVD_DT,
        LAST_RCVD_DT,
        FIRST_SOLD_DT,
        LAST_SOLD_DT,
        RMS_Q_SKU_DNUM,
        RMS_Q_SEASON_CDE,
        RMS_Q_STY_IMG,
        RMS_Q_STY_IMG_S,
        RMS_Q_SKU_SDESC,
        RMS_Q_ORIG_UNIT_RTL,
        RMS_Q_STY_IMG_L,
        RMS_ITEM_NBR_TYPE_CDE,
        RMS_ITEM_SECND_DESC,
        RMS_LEVEL3_IDNT,
        RMS_TRAN_LEVEL,
        RMS_Q_STATUS_FLAG,
        RMS_SRC_ID,
        RMS_Q_DIM_SNUM,
        RMS_Q_VND_SNUM,
        RMS_Q_SKU_IMG,
        RMS_LEVEL2_IDNT,
        RMS_LOAD_DATE,
        RMS_Q_CURR_UNIT_RTL,
        RMS_Q_CURR_PRICE_STATUS

    FROM {{ref('TMP_D_PRD_ITM_LU')}}
),

updates as
(
    SELECT
        ITM_KEY,
        ITM_ID,
        ITM_DESC,
        STY_KEY,
        STY_ID,
        STY_DESC,
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
        UPC_ID,
        --SUP_KEY,
        --SUP_ID,
        --SUP_PART_ID,
        --SUP_DESC,
        COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        MER_IND,
        FCST_IND,
        INV_IND,
        PACK_IND,
        ORDERABLE_IND,
        SELLABLE_IND,
        SIMPLE_PACK_IND,
        STND_UOM_CDE,
        FIRST_RCVD_DT,
        LAST_RCVD_DT,
        FIRST_SOLD_DT,
        LAST_SOLD_DT,
        RMS_Q_SKU_DNUM,
        RMS_Q_SEASON_CDE,
        RMS_Q_STY_IMG,
        RMS_Q_STY_IMG_S,
        RMS_Q_SKU_SDESC,
        RMS_Q_ORIG_UNIT_RTL,
        RMS_Q_STY_IMG_L,
        RMS_ITEM_NBR_TYPE_CDE,
        RMS_ITEM_SECND_DESC,
        RMS_LEVEL3_IDNT,
        RMS_TRAN_LEVEL,
        RMS_Q_STATUS_FLAG,
        RMS_SRC_ID,
        RMS_Q_DIM_SNUM,
        RMS_Q_VND_SNUM,
        RMS_Q_SKU_IMG,
        RMS_LEVEL2_IDNT,
        RMS_LOAD_DATE,
        RMS_Q_CURR_UNIT_RTL,
        RMS_Q_CURR_PRICE_STATUS,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause

    {% if is_incremental() %}
    WHERE ITM_ID in (SELECT ITM_ID from {{this}})
    {% endif %}

),

inserts as
(
    SELECT 
        ITM_KEY,
        ITM_ID,
        ITM_DESC,
        STY_KEY,
        STY_ID,
        STY_DESC,
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
        UPC_ID,
        --SUP_KEY,
        --SUP_ID,
        --SUP_PART_ID,
        --SUP_DESC,
        COLOR_KEY,
        COLOR_ID,
        COLOR_DESC,
        SIZE_KEY,
        SIZE_ID,
        SIZE_DESC,
        MER_IND,
        FCST_IND,
        INV_IND,
        PACK_IND,
        ORDERABLE_IND,
        SELLABLE_IND,
        SIMPLE_PACK_IND,
        STND_UOM_CDE,
        FIRST_RCVD_DT,
        LAST_RCVD_DT,
        FIRST_SOLD_DT,
        LAST_SOLD_DT,
        RMS_Q_SKU_DNUM,
        RMS_Q_SEASON_CDE,
        RMS_Q_STY_IMG,
        RMS_Q_STY_IMG_S,
        RMS_Q_SKU_SDESC,
        RMS_Q_ORIG_UNIT_RTL,
        RMS_Q_STY_IMG_L,
        RMS_ITEM_NBR_TYPE_CDE,
        RMS_ITEM_SECND_DESC,
        RMS_LEVEL3_IDNT,
        RMS_TRAN_LEVEL,
        RMS_Q_STATUS_FLAG,
        RMS_SRC_ID,
        RMS_Q_DIM_SNUM,
        RMS_Q_VND_SNUM,
        RMS_Q_SKU_IMG,
        RMS_LEVEL2_IDNT,
        RMS_LOAD_DATE,
        RMS_Q_CURR_UNIT_RTL,
        RMS_Q_CURR_PRICE_STATUS,
        CURRENT_TIMESTAMP as RCD_INS_TS,
        CURRENT_TIMESTAMP as RCD_UPD_TS,
        0 as RCD_CLOSE_FLG,
        '9999-12-31' as RCD_CLOSE_DT
    FROM using_clause
    WHERE ITM_ID not in (SELECT ITM_ID from updates)
)


select * from updates {% if is_incremental() %} union SELECT * FROM inserts {% endif %} 
