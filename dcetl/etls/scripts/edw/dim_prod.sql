TRUNCATE TABLE edw.dim_prod;
INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days,  -- 开放天数
    end_dt_calc_way,-- 到期日计算方式
    expc_onln_dtm,  -- 预约上架时间
    rerd_dtm,       -- 备案日期
    prod_nm_cd,          -- 名称代码
    last_pay_dt,    -- 最后打款日
    fst_raise_beg_dt,    -- 首次募集开始日
    last_raise_end_dt,   -- 最新募集结束日
    elec_sign_flag ,        -- 是否支持电子签署
    prod_proc_tag
)
WITH temp AS (
    SELECT
        p.prod_id,
        min(p.beg_date) AS fst_raise_beg_dt,     -- 首次募集开始日
        max(p.end_date) AS last_raise_end_dt,    -- 最新募集结束日
        sum(p.open_days) AS open_days           -- 产品开放天数(理财师实际募集天数，只取工作日)
    FROM (
        -- 募集期产品
        SELECT
            a.id AS prod_id,
            a.rais_beg_date AS beg_date,
            a.rais_end_date AS end_date,
            -- 当前日期>结束日期 取结束日期-开始日期 (开始日期不存在则取募集结束日期) 反之 取当前日期减开始日期
            CASE WHEN CURRENT_DATE >= a.rais_end_date
                THEN work_days_rng_dts(COALESCE(a.rais_beg_date, rais_end_date)::date, a.rais_end_date::date)
                WHEN a.rais_beg_date IS NULL AND a.rais_end_date IS NULL THEN 0
                ELSE work_days_rng_dts(COALESCE(a.rais_beg_date::date, CURRENT_DATE), CURRENT_DATE)
                END AS open_days
        FROM ods.pf_prod_info a
        WHERE a.rec_stat='1'
      UNION ALL
        -- 产品成立后，再次开放的产品
        SELECT
            b.id AS prod_id,
            a.open_beg_date AS beg_date,
            a.open_end_date AS end_date,
            CASE WHEN CURRENT_DATE >= a.order_end_date
                THEN work_days_rng_dts(COALESCE(a.order_beg_date, order_end_date)::date, a.order_end_date::date)
                WHEN a.order_beg_date IS NULL AND a.order_end_date IS NULL THEN 0
                ELSE work_days_rng_dts(COALESCE(a.order_beg_date::date, CURRENT_DATE), CURRENT_DATE)
                END AS open_days
        FROM ods.pf_prod_open_days a, ods.pf_prod_info b
        WHERE a.prod_id=b.id AND a.rec_stat='1' AND b.rec_stat='1'
            AND a.sg_stat='1'
    ) AS p
    GROUP BY p.prod_id
)
SELECT
    'pf_'|| prd.id::varchar AS prod_id,     -- 唯一产品id
    prd.id::varchar         AS src_prod_id, -- 源系统产品id
    'pf'                    AS src_sys_cd,  -- 源系统编号
    prd.prod_name           AS prod_nm,     -- 产品名称
    prd.prod_code           AS prod_cd,     -- 产品编号
--     NULL                    AS shr_type_nm, -- 份额类别名称,
    '私募产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm              AS cat_2nd_nm,  -- 二级分类
    pk3.cat_nm              AS cat_3rd_nm,  -- 三级分类
    prd.prod_alia,  -- 产品简称
    m2.dict_nm               AS prod_risk_lvl,   -- 产品风险等级
    prd.tot_amt             AS raise_amnt,    -- 募集金额
    prd.prod_esta_date      AS prod_esta_dt,-- 产品成立日期
    prd.rais_beg_date       AS raise_beg_dt, -- 募集开始日期
    prd.rais_end_date       AS raise_end_dt, -- 募集结束日期
    mgr.mgr_name            AS mgr_nm, -- 产品管理人
    COALESCE(mgr2.mgr_name,mgr.mgr_name)    AS mop_mgr_nm, -- 业绩归属管理人
    CASE WHEN prd.esta_stat='1' THEN '成立' ELSE '未成立' END    AS esta_stat, -- 成立状态
    m3.dict_nm               AS online_stat, -- 上架状态
    m7.dict_nm               AS cstd,        -- 托管人
    prd.electronic_cont     AS elec_cont_flag,  -- 是否用电子合同
    m4.dict_nm               AS oper_type,   -- 运作方式
    m5.dict_nm               AS sale_type,   -- 销售方式
    m1.dict_nm               AS crcy_type,   -- 货币类型
    m6.dict_nm               AS conf_stat,
    CASE WHEN prd.fast_redm_flg='1' THEN '支持' ELSE '不支持' END    AS fast_redm_flag,  -- 快速赎回标识
    CASE WHEN CURRENT_DATE>prd.rais_end_date THEN '募集结束' ELSE '募集中' END AS raise_stat,  -- 募集状态
    prd.tot_cont            AS cont_cnt,    -- 合同数
    prd.expe_online_date    AS expc_onln_dt,
    COALESCE(mgr3.group_name, '其他板块') mgr_group,
    t.open_days             AS acc_open_days,   -- 开放天数
    dict.dict_nm           AS end_dt_calc_way, -- 到期日计算方式
    prd.pre_online_time     AS expc_onln_dtm,   -- 预约上架时间
    prd.record_date         AS rerd_dtm,        -- 备案日期
    prd.prod_abbr           AS prod_nm_cd,           -- 名称代码
    prd.last_capi_day       AS last_pay_dt,     -- 最后打款日
    t.fst_raise_beg_dt,
    t.last_raise_end_dt,
    CASE prd.online_sign WHEN '1' THEN 'Y' ELSE 'N' END AS elec_sign_flag,
    case when prd.sale_org='001' then '海银基金代销'
         when ic.id>0 and prd.sale_type='1' then '管理人对接直销'
         when prd.id in(170274) then '补录产品'
         else '其他' end prod_proc_tag
FROM ods.pf_prod_info      prd
LEFT JOIN ods.pf_mgr_info  mgr ON prd.mgr_code = mgr.mgr_code AND mgr.rec_stat='1'
LEFT JOIN mapp.mgr_info    mgr2 ON prd.mgr_sys_id = mgr2.mgr_sys_id
LEFT JOIN dd.mgr_group     mgr3 ON prd.plate = mgr3.plate
LEFT JOIN edw.dim_prod_cat   pk ON 'pf_'||prd.earn_type = pk.cat_id
LEFT JOIN edw.dim_prod_cat  pk2 ON 'pf_'||prd.earn_type||'_'||prd.prod_type = pk2.cat_id
LEFT JOIN edw.dim_prod_cat  pk3 ON 'pf_'||prd.earn_type||'_'||prd.prod_type||'_'||prd.three_class = pk3.cat_id
LEFT JOIN edw.dict_src_sys m1 ON m1.sys_alia_cd='pf'
    AND m1.group_cd='CURR_TYPE' AND m1.dict_key=prd.curr_type
LEFT JOIN edw.dict_src_sys m2 ON m2.sys_alia_cd='pf'
    AND m2.group_cd='RISK_LVL' AND m2.dict_key=prd.prod_risk_level
LEFT JOIN edw.dict_src_sys m3 ON m3.sys_alia_cd='pf'
    AND m3.group_cd='PRD_PRIVATE_ONLINE_STAT' AND m3.dict_key=prd.online_stat
LEFT JOIN edw.dict_src_sys m4 ON m4.sys_alia_cd='pf'
    AND m4.group_cd='PRD_PRIVATE_OPER_TYPE' AND m4.dict_key=prd.oper_type
LEFT JOIN edw.dict_src_sys m5 ON m5.sys_alia_cd='pf'
    AND m5.group_cd='SALE_TYPE' AND m5.dict_key=prd.sale_type
LEFT JOIN edw.dict_src_sys m6 ON m6.sys_alia_cd='pf'
    AND m6.group_cd='CONF_STAT' AND m6.dict_key=prd.conf_stat
LEFT JOIN edw.dict_src_sys m7 ON m7.sys_alia_cd='pf'
    AND m7.group_cd='TA_CODE' AND m7.dict_key=prd.TA_CODE
LEFT JOIN dw.dict_map      dict ON dict.dict_key=prd.cash_flag
    AND dict.grp_key='cash_flag' AND dict.src_sys_cd='sp'
LEFT JOIN temp t ON prd.id=t.prod_id
LEFT JOIN ods.pf_mgr_inter_config ic on prd.mgr_sys_id=ic.id
WHERE prd.rec_stat='1';

INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days,  -- 开放天数
    onln_dt         -- 上线日期
)
SELECT
    'hyf_'||prd.fundcode    AS prod_id,     -- 唯一产品id
    prd.fundcode            AS src_prod_id, -- 源系统产品id
    'hyf'                   AS src_sys_cd,  -- 源系统编号
    prd.fundnameabbr        AS prod_nm,     -- 产品名称(暂时取简称 基金运营部要求)
    prd.fundcode            AS prod_cd,     -- 产品编号
--     NULL shr_type_nm, -- 份额类别名称
    '公募产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm              AS cat_2nd_nm,  -- 二级分类
    NULL                    AS cat_3rd_nm,  -- 三级分类
    prd.fundnameabbr        AS prod_alia,   -- 产品简称
    m.dict_nm||'风险'          AS prod_risk_lvl,   -- 产品风险等级
    NULL raise_amnt, -- 募集金额
    to_date(prd.ipostartdate, 'yyyymmdd')   AS prod_esta_dt, -- 产品成立日期
    NULL rais_beg_date, -- 募集开始日期
    NULL rais_end_date, -- 募集结束日期
    NULL mgr_nm, -- 产品管理人
    NULL mop_mgr_nm, -- 业绩归属管理人
    '无成立状态'                 AS esta_stat,   -- 成立状态
    '无上架状态'                 AS online_stat, -- 上架状态
    NULL cstd, -- 托管人
    NULL elec_cont_flag, -- 是否用电子合同
    NULL oper_type, -- 运作方式
    NULL sale_type, -- 销售方式
    NULL crcy_type, -- 货币类型
    NULL conf_stat,
    NULL fast_redm_flag,  -- 快速赎回标识
    NULL raise_stat,  -- 募集状态
    NULL cont_cnt, -- 合同数
    NULL expc_onln_dt,
    '多元板块'                  AS mgr_group,
    NULL acc_open_days,
    (CASE WHEN prd.createdate='' THEN NULL ELSE prd.createdate END)::date as onln_dt
FROM ods.hyf_batch_basic_fundinfo prd
LEFT JOIN edw.dim_prod_cat  pk ON pk.cat_id='hyf_y'
LEFT JOIN edw.dim_prod_cat pk2 ON 'hyf_y_'||prd.fundtype = pk2.cat_id
LEFT JOIN edw.dict_src_sys m ON m.group_cd ='PRD_RISKLEVEL'
    AND m.sys_alia_cd='hyf' AND prd.fundrisklevel=m.dict_key
WHERE LEFT(prd.fundtype, 1)='2';

INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days,  -- 开放天数
    fst_raise_beg_dt,    -- 首次募集开始日
    last_raise_end_dt    -- 最新募集结束日
)
-- 海外基金产品
SELECT
    'of_'|| prd.id::varchar AS prod_id,     -- 唯一产品id
     prd.id::varchar        AS src_prod_id, -- 源系统产品id
    'of'                    AS src_sys_cd,  -- 源系统编号
    prd.prod_name           AS prod_nm,     -- 产品名称
    prd.prod_code           AS prod_cd,     -- 产品编号
--     NULL::varchar(100)      AS shr_type_nm, -- 份额类别名称
    '多元产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm              AS cat_2nd_nm,  -- 二级分类
    NULL                    AS cat_3rd_nm,  -- 三级分类
    prd.prod_alia, -- 产品简称
    m2.dict_nm||'风险'         AS prod_risk_lvl,   -- 产品风险等级
    prd.tot_amt/10000       AS raise_amnt,    -- 募集金额
    prd.prod_esta_date      AS prod_esta_dt,-- 产品成立日期
    prd.rais_beg_date       AS raise_beg_dt, -- 募集开始日期
    prd.rais_end_date       AS raise_end_dt, -- 募集结束日期
    NULL::varchar(40)       AS mgr_nm, -- 产品管理人
    NULL::varchar(40)       AS mop_mgr_nm, -- 业绩归属管理人
    CASE WHEN prd.esta_stat='1' THEN '成立' ELSE '未成立' END    AS esta_stat, -- 成立状态
    CASE WHEN sale_stat='0' THEN '下架' WHEN sale_stat='1' THEN '上架' ELSE '无上架状态' END AS online_stat, -- 上架状态(海外基金私募系统显示在售、停售)
    NULL::varchar(100)      AS cstd,        -- 托管人
    NULL::varchar(2)        AS elec_cont_flag, -- 是否用电子合同
    m4.dict_nm               AS oper_type,   -- 运作方式
    NULL::varchar(40)       AS sale_type,   -- 销售方式
    m1.dict_nm               AS crcy_type,   -- 货币类型
    m6.dict_nm               AS conf_stat,
    CASE WHEN prd.fast_redm_flg='1' THEN '支持' ELSE '不支持' END fast_redm_flag,  -- 快速赎回标识
    CASE WHEN CURRENT_DATE > prd.rais_end_date THEN '募集结束' ELSE '募集中' END raise_stat,  -- 募集状态
    NULL::int4              AS cont_cnt,    -- 合同数
    NULL::date              AS expc_onln_dt,
    '多元板块'              AS mgr_group,
    NULL::int8              AS acc_open_days,
    prd.rais_beg_date fst_raise_beg_dt,
    prd.rais_end_date last_raise_end_dt
FROM ods.of_prod_info           prd
LEFT JOIN edw.dim_prod_cat      pk ON pk.cat_id='other_f'
LEFT JOIN edw.dim_prod_cat     pk2 ON pk2.cat_id='other_f_1'
LEFT JOIN edw.dict_src_sys    m1 ON m1.sys_alia_cd='of'
    AND m1.group_cd='CURR_TYPE' AND m1.dict_key=prd.curr_type
LEFT JOIN edw.dict_src_sys    m2 ON m2.sys_alia_cd='of'
    AND m2.group_cd='RISK_LVL' AND m2.dict_key=prd.prod_risk_level
LEFT JOIN edw.dict_src_sys    m4 ON m4.sys_alia_cd='of'
    AND m4.group_cd='PRD_PRIVATE_OPER_TYPE' AND m4.dict_key=prd.oper_type
LEFT JOIN edw.dict_src_sys    m6 ON m6.sys_alia_cd='of'
    AND m6.group_cd='PROD_CONF_STAT' AND m6.dict_key=prd.conf_stat
WHERE prd.rec_stat='1'
UNION ALL
-- 海外置业产品
SELECT
    'of_pur_'|| prd.id::varchar AS prod_id,     -- 唯一产品id
     prd.id::varchar    AS src_prod_id, -- 源系统产品id
    'of'                AS src_sys_cd,  -- 源系统编号
    prd.purchase_name   AS prod_nm,     -- 产品名称
    prd.purchase_code   AS prod_cd,     -- 产品编号
--     NULL shr_type_nm,   -- 份额类别名称
    '多元产品'           AS prod_type,   -- 产品大类
    pk.cat_nm           AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm          AS cat_2nd_nm,  -- 二级分类
    NULL                AS cat_3rd_nm,  -- 三级分类
    prd.purchase_name   AS prod_alia, -- 产品简称
    '无风险等级'             AS prod_risk_lvl,   -- 产品风险等级
    NULL raise_amnt,      -- 募集金额
    NULL prod_esta_dt,  -- 产品成立日期
    NULL raise_beg_dt,   -- 募集开始日期
    NULL raise_end_dt,   -- 募集结束日期
    NULL mgr_nm,        -- 产品管理人
    NULL mop_mgr_nm,   -- 业绩归属管理人
    '无成立状态'             AS esta_stat,   -- 成立状态
    CASE WHEN sale_stat='0' THEN '下架' WHEN sale_stat='1' THEN '上架' ELSE '无上架状态' END AS online_stat, -- 上架状态
    NULL cstd,          -- 托管人
    NULL elec_cont_flag,-- 是否用电子合同
    NULL oper_type,     -- 运作方式
    NULL sale_type,     -- 销售方式
    m1.dict_nm           AS crcy_type,   -- 货币类型
    m6.dict_nm           AS conf_stat,
    NULL fast_redm_flag,-- 快速赎回标识
    NULL raise_stat,     -- 募集状态
    NULL cont_cnt,      -- 合同数
    NULL expc_onln_dt,
    '多元板块'              AS mgr_group,
    NULL acc_open_days,
    NULL fst_raise_beg_dt,
    NULL last_raise_end_dt
FROM ods.of_purchase            prd
LEFT JOIN edw.dim_prod_cat      pk ON pk.cat_id='other_f'
LEFT JOIN edw.dim_prod_cat     pk2 ON pk2.cat_id='other_f_2'
LEFT JOIN edw.dict_src_sys    m1 ON m1.sys_alia_cd='of'
    AND m1.group_cd='CURR_TYPE' AND m1.dict_key=prd.curr_type
LEFT JOIN edw.dict_src_sys    m6 ON m6.sys_alia_cd='of'
    AND m6.group_cd='CONF_STAT' AND m6.dict_key=prd.conf_stat
WHERE prd.rec_stat='1'
UNION ALL
-- 海外移民产品
SELECT
    'of_immig_'|| prd.id::varchar AS prod_id,     -- 唯一产品id
     prd.id::varchar    AS src_prod_id, -- 源系统产品id
    'of'                AS src_sys_cd,  -- 源系统编号
    prd.immigrant_name  AS prod_nm,     -- 产品名称
    prd.immigrant_code  AS prod_cd,     -- 产品编号
--     NULL shr_type_nm,   -- 份额类别名称
    '多元产品'           AS prod_type,   -- 产品大类
    pk.cat_nm           AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm          AS cat_2nd_nm,  -- 二级分类
    NULL                AS cat_3rd_nm,  -- 三级分类
    prd.immigrant_name  AS prod_alia, -- 产品简称
    '无风险等级'             AS prod_risk_lvl,   -- 产品风险等级
    NULL raise_amnt,      -- 募集金额
    NULL prod_esta_dt,  -- 产品成立日期
    NULL raise_beg_dt,   -- 募集开始日期
    NULL raise_end_dt,   -- 募集结束日期
    NULL mgr_nm,        -- 产品管理人
    NULL mop_mgr_nm,   -- 业绩归属管理人
    '无成立状态'             AS esta_stat,   -- 成立状态
    CASE WHEN sale_stat='0' THEN '下架' WHEN sale_stat='1' THEN '上架' ELSE '无上架状态' END AS online_stat, -- 上架状态
    NULL cstd,          -- 托管人
    NULL elec_cont_flag,-- 是否用电子合同
    NULL oper_type,     -- 运作方式
    NULL sale_type,     -- 销售方式
    m1.dict_nm           AS crcy_type,   -- 货币类型
    m6.dict_nm           AS conf_stat,
    NULL fast_redm_flag,-- 快速赎回标识
    NULL raise_stat,     -- 募集状态
    NULL cont_cnt,      -- 合同数
    NULL expc_onln_dt,
    '多元板块'              AS mgr_group,
    NULL acc_open_days,
    NULL fst_raise_beg_dt,
    NULL last_raise_end_dt
FROM ods.of_immigrant            prd
LEFT JOIN edw.dim_prod_cat      pk ON pk.cat_id='other_f'
LEFT JOIN edw.dim_prod_cat     pk2 ON pk2.cat_id='other_f_3'
LEFT JOIN edw.dict_src_sys    m1 ON m1.sys_alia_cd='of'
    AND m1.group_cd='CURR_TYPE' AND m1.dict_key=prd.curr_type
LEFT JOIN edw.dict_src_sys    m6 ON m6.sys_alia_cd='of'
    AND m6.group_cd='CONF_STAT' AND m6.dict_key=prd.conf_stat
WHERE prd.rec_stat='1';

INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days   -- 开放天数
)
SELECT
    'ins_'|| prd.id::varchar AS prod_id,     -- 唯一产品id
    prd.id::varchar         AS src_prod_id, -- 源系统产品id
    'pf'                    AS src_sys_cd,  -- 源系统编号
    prd.prod_name           AS prod_nm,     -- 产品名称
    prd.prod_code           AS prod_cd,     -- 产品编号
--     NULL shr_type_nm, -- 份额类别名称
    '多元产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    pk2.cat_nm              AS cat_2nd_nm,  -- 二级分类
    NULL                    AS cat_3rd_nm,  -- 三级分类
    prd.prod_alia, -- 产品简称
    '无风险等级'                 AS prod_risk_lvl,   -- 产品风险等级
    NULL raise_amnt, -- 募集金额
    prd.prod_beg_date       AS prod_esta_dt,-- 产品成立日期
    prd.pub_beg_date        AS raise_beg_dt, -- 募集开始日期
    prd.pub_end_date        AS raise_end_dt, -- 募集结束日期
    NULL mgr_nm, -- 产品管理人
    NULL mop_mgr_nm, -- 业绩归属管理人
    '无成立状态'             AS esta_stat,   -- 成立状态
    '无上架状态'             AS online_stat, -- 上架状态
    NULL cstd, -- 托管人
    NULL elec_cont_flag, -- 是否用电子合同
    NULL oper_type, -- 运作方式
    NULL sale_type, -- 销售方式
    NULL crcy_type, -- 货币类型
    NULL conf_stat,
    NULL fast_redm_flag,  -- 快速赎回标识
    CASE WHEN CURRENT_DATE > prd.pub_end_date THEN '募集结束' ELSE '募集中' END    AS raise_stat,  -- 募集状态
    NULL cont_cnt, -- 合同数
    NULL expc_onln_dt,
    '多元板块'              AS mgr_group,
     CASE WHEN CURRENT_DATE >= prd.pub_end_date
        THEN work_days_rng_dts(COALESCE(prd.pub_beg_date, pub_end_date), prd.pub_end_date)
        WHEN prd.pub_beg_date IS NULL AND prd.pub_end_date IS NULL THEN 0
        ELSE work_days_rng_dts(COALESCE(prd.pub_beg_date, CURRENT_DATE), CURRENT_DATE)
        END              AS acc_open_days
FROM ods.pf_hpf_safe_prod prd
LEFT JOIN edw.dim_prod_cat  pk ON pk.cat_id='other_i'
LEFT JOIN edw.dim_prod_cat pk2 ON pk2.cat_id='other_i_'||prd.insurance_type
WHERE prd.rec_stat='1';

INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days   -- 开放天数
)
SELECT
    'hyh_hyh' AS prod_id, -- 唯一产品id
    'hyh' AS src_prod_id, -- 源系统产品id
    'hyh' AS src_sys_cd,        -- 源系统编号
    'HYH产品' AS prod_nm, -- 产品名称
    'HYH' AS prod_cd, -- 产品编号
--     NULL shr_type_nm, -- 份额类别名称
    '多元产品'  AS prod_type,   -- 产品大类
    'HYH'   AS cat_1st_nm, -- 一级分类
    NULL    AS cat_2nd_nm, -- 二级分类
    NULL    AS cat_3rd_nm,  -- 三级分类
    'HYH产品' AS product_name, -- 产品简称
    '无风险等级' AS prod_risk_lvl, -- 产品风险等级
    NULL raise_amnt, -- 募集金额
    NULL prod_esta_dt, -- 产品成立日期
    NULL rais_beg_date, -- 募集开始日期
    NULL rais_end_date, -- 募集结束日期
    NULL mgr_nm, -- 产品管理人
    NULL mop_mgr_nm, -- 业绩归属管理人
    '无成立状态' AS esta_stat, -- 成立状态
    '无上架状态' AS online_stat, -- 上架状态
    NULL cstd, -- 托管人
    NULL elec_cont_flag, -- 是否用电子合同
    NULL oper_type, -- 运作方式
    NULL sale_type, -- 销售方式
    NULL crcy_type, -- 货币类型
    NULL conf_stat,
    NULL fast_redm_flag,  -- 快速赎回标识
    NULL raise_stat,  -- 募集状态
    NULL cont_cnt, -- 合同数
    NULL expc_onln_dt,
    '多元板块' AS mgr_group,
    NULL acc_open_days
;

INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days,  -- 开放天数
    prod_std_kpi      -- YQD产品系数
)
SELECT
    'yqd_'||TRIM(data->>'productNo')    AS prod_id,     -- 唯一产品id
    TRIM(data->>'productNo')            AS src_prod_id, -- 源系统产品id
    'yqd'                       AS src_sys_cd,  -- 源系统编号
    TRIM(data->>'productTitle') AS prod_nm,     -- 产品名称
    TRIM(data->>'productNo')    AS prod_cd,     -- 产品编号
--     NULL                    AS /, -- 份额类别名称
    '多元产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    NULL                    AS cat_2nd_nm,  -- 二级分类
    NULL                    AS cat_3rd_nm,  -- 三级分类
    TRIM(data->>'productTitle') AS prod_alia, -- 产品简称
    '无风险等级'             AS prod_risk_lvl, -- 产品风险等级
    NULL raise_amnt, -- 募集金额
    NULL prod_esta_dt,  -- 产品成立日期
    NULL rais_beg_date, -- 募集开始日期
    NULL rais_end_date, -- 募集结束日期
    NULL mgr_nm, -- 产品管理人
    NULL mop_mgr_nm, -- 业绩归属管理人
    '无成立状态'             AS esta_stat,   -- 成立状态
    '无上架状态'             AS online_stat, -- 上架状态
    NULL cstd, -- 托管人
    NULL elec_cont_flag, -- 是否用电子合同
    NULL oper_type, -- 运作方式
    NULL sale_type, -- 销售方式
    NULL crcy_type, -- 货币类型
    NULL conf_stat,
    NULL fast_redm_flag,  -- 快速赎回标识
    NULL raise_stat,  -- 募集状态
    NULL cont_cnt, -- 合同数
    NULL expc_onln_dt,
    '多元板块'              AS mgr_group,
    NULL acc_open_days,
    TRIM(data->>'productModulus')::numeric(10,4)    AS prod_std_kpi   -- YQD产品系数
FROM ods.yqd_prod prd
LEFT JOIN edw.dim_prod_cat  pk ON pk.cat_id='other_yqd'
;


INSERT INTO edw.dim_prod(
    prod_id,        -- 唯一产品id
    src_prod_id,    -- 源系统产品id
    src_sys_cd,     -- 源系统编号
    prod_nm,        -- 产品名称
    prod_cd,        -- 产品编号
--     shr_type_nm,    -- 份额类别名称
    prod_type,      -- 产品大类
    cat_1st_nm,     -- 一级分类
    cat_2nd_nm,     -- 二级分类
    cat_3rd_nm,     -- 三级分类
    prod_alia,      -- 产品简称
    prod_risk_lvl,  -- 产品风险等级
    raise_amnt,       -- 募集金额
    prod_esta_dt,   -- 产品成立日期
    raise_beg_dt,    -- 募集开始日期
    raise_end_dt,    -- 募集结束日期
    mgr_nm,         -- 产品管理人
    mop_mgr_nm,    -- 业绩归属管理人
    esta_stat,      -- 成立状态
    online_stat,    -- 上架状态
    cstd,           -- 托管人
    elec_cont_flag, -- 是否用电子合同
    oper_type,      -- 运作方式
    sale_type,      -- 销售方式
    crcy_type,      -- 货币类型
    conf_stat,
    fast_redm_flag, -- 快速赎回标识
    raise_stat,
    cont_cnt,
    expc_onln_dt,
    mgr_group,      -- 所属板块
    acc_open_days   -- 开放天数
)
SELECT
    'yqd_'|| prd.product_id AS prod_id,     -- 唯一产品id
     prd.product_id         AS src_prod_id, -- 源系统产品id
    'yqd'                   AS src_sys_cd,  -- 源系统编号
    prd.product_name        AS prod_nm,     -- 产品名称
    prd.product_id          AS prod_cd,     -- 产品编号
--     NULL shr_type_nm, -- 份额类别名称
    '多元产品'               AS prod_type,   -- 产品大类
    pk.cat_nm               AS cat_1st_nm,  -- 一级分类
    NULL                    AS cat_2nd_nm,  -- 二级分类
    NULL                    AS cat_3rd_nm,  -- 三级分类
    prd.product_name, -- 产品简称
    '无风险等级'             AS prod_risk_lvl, -- 产品风险等级
    NULL raise_amnt, -- 募集金额
    NULL prod_esta_dt, -- 产品成立日期
    NULL rais_beg_date, -- 募集开始日期
    NULL rais_end_date, -- 募集结束日期
    NULL mgr_nm, -- 产品管理人
    NULL mop_mgr_nm, -- 业绩归属管理人
    '无成立状态'             AS esta_stat,   -- 成立状态
    '无上架状态'             AS online_stat, -- 上架状态
    NULL cstd, -- 托管人
    NULL elec_cont_flag, -- 是否用电子合同
    NULL oper_type, -- 运作方式
    NULL sale_type, -- 销售方式
    NULL crcy_type, -- 货币类型
    NULL conf_stat,
    NULL fast_redm_flag,  -- 快速赎回标识
    NULL raise_stat,  -- 募集状态
    NULL cont_cnt, -- 合同数
    NULL expc_onln_dt,
    '多元板块'              AS mgr_group,
    NULL acc_open_days
FROM (
    SELECT product_id, min(product_name) AS product_name
    FROM (
        SELECT product_id, product_name
        FROM hyh.offline_invapi006_his
      UNION
        SELECT product_id, product_name
        FROM hyh.offline_invapi006
        WHERE trans_date>='2018-09-01'
    ) a
    GROUP BY product_id
) prd
LEFT JOIN edw.dim_prod_cat  pk ON pk.cat_id='other_yqd'
WHERE NOT EXISTS (SELECT 1 FROM edw.dim_prod dim WHERE 'yqd_'|| prd.product_id=dim.prod_id)