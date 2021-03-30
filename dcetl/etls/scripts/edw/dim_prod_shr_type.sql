truncate table edw.dim_prod_shr_type;
INSERT INTO edw.dim_prod_shr_type (
        prod_shr_type_id,
        src_shr_type_id,
        prod_id,
        shr_type_cd,
        shr_type_nm,
        pay_intst_type,
        prft_calc_type,
        kpi,
        std_kpi,    -- 标准业绩系数
        is_stn_flag, -- 永续标识
        prod_shr_end_dt,
        expc_expr_dt,
        real_expr_dt,
        term_unit,
        min_ddl,
        max_ddl,
        invst_term_tag,
        open_all_fp,
        src_sys_cd
    )
    WITH tp_a AS (-- 类固收 现金管理类
        SELECT
            d.id AS shr_type_id,
            p.prod_type,
            CASE WHEN DEADLINE_UNIT='2' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/365, 2)
                 WHEN DEADLINE_UNIT='1' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/12, 2)
                 ELSE round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC), 2) END AS deadline
        FROM ods.pf_prod_expi_date d
        LEFT JOIN ods.pf_prod_info p ON d.prod_id=p.id
        WHERE p.rec_stat='1' AND d.rec_stat='1' AND p.earn_type IN ('1','5')
    ),
    tp_b AS ( -- 权益类无封闭期
        SELECT
            d.id AS shr_type_id,
            d.prod_id,
            CASE WHEN DEADLINE_UNIT='2' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/365, 2)
                 WHEN DEADLINE_UNIT='1' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/12, 2)
                 ELSE round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC), 2) END AS deadline
        FROM ods.pf_prod_expi_date d
        LEFT JOIN ods.pf_prod_info p ON d.prod_id=p.id
        WHERE p.rec_stat='1' AND d.rec_stat='1' AND p.earn_type IN ('2','3','4')
            AND (d.close_time IS NULL OR d.close_time=0)
    ),
    tp_c AS (-- 权益类有封闭期
        SELECT
            d.id AS shr_type_id,
            round( CASE WHEN unit='1' THEN COALESCE(close_time, '0')::numeric/365
                        WHEN unit='2' THEN COALESCE(close_time, '0')::numeric/12
                        ELSE COALESCE(close_time,'0')::numeric END, 2 ) AS close_time
        FROM ods.pf_prod_expi_date d
        LEFT JOIN ods.pf_prod_info p ON d.prod_id=p.id
        WHERE p.rec_stat='1' AND d.rec_stat='1' AND p.earn_type IN ('2','3','4') AND d.close_time>0
    ),
    tp_e AS (-- 信托产品 期限*0.3
        SELECT
            d.id AS shr_type_id,
            p.prod_type,
            CASE WHEN DEADLINE_UNIT='2' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/365, 2)
                 WHEN DEADLINE_UNIT='1' THEN round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC)/12, 2)
                 ELSE round((COALESCE(min_deadline, '0')::NUMERIC + COALESCE(exit_line, '0')::NUMERIC), 2) END AS deadline
        FROM ods.pf_prod_expi_date d
        LEFT JOIN ods.pf_prod_info p ON d.prod_id=p.id
        WHERE p.rec_stat='1' AND d.rec_stat='1' AND p.earn_type='6'
    ),
    tp_d AS ( -- 产品std_kpi汇总
        SELECT shr_type_id, CASE WHEN prod_type='51' THEN 0 ELSE deadline END std_kpi FROM tp_a
      UNION ALL
        SELECT shr_type_id, CASE WHEN deadline>5 THEN 5  ELSE deadline END std_kpi FROM tp_b
      UNION ALL
        SELECT
            shr_type_id,
            CASE WHEN close_time<=0.5 THEN 0.5
                 WHEN close_time>1 THEN close_time
                 WHEN close_time IS NULL THEN 0
                 ELSE 1 END AS std_kpi
        FROM tp_c
      UNION ALL
        SELECT shr_type_id, deadline*0.3 std_kpi FROM tp_e
    )
    SELECT
        'pf_'||p.id::varchar        AS shr_type_id,
        p.id::varchar               AS src_shr_type_id,
        'pf_'|| prod_id::varchar    AS prod_id ,
        p.shr_type_code             AS shr_type_cd, -- 份额类别编码
        p.shr_type                  AS shr_type_nm, -- 份额类别名称
        m6.dict_nm                   AS pay_intst_type , -- 付息方式
        m5.dict_nm                   AS prft_calc_type,--  收益计算方式
        p.kpi, -- kpi系数
        COALESCE(tp_d.std_kpi,0)    AS std_kpi,
        CASE WHEN p.sustain='1' THEN 'Y' ELSE 'N' END   AS is_stn_flag, -- 是否永续
        p.prod_end_date             AS prod_shr_end_dt, -- 产品份额类别到期日
        p.expe_expi_date            AS expc_expr_dt, -- 预估到期日
        p.real_expi_date            AS real_expr_dt,  -- 实际到期日
        m4.dict_nm                   AS term_unit , --期限单位
        p.min_deadline              AS min_ddl,  -- 最小期限
        p.max_deadline              AS max_ddl,
        CASE WHEN m4.dict_nm='月' AND p.min_deadline::int>=12 AND p.min_deadline::int<24 THEN '1年-2年'
             WHEN m4.dict_nm='月' AND p.min_deadline::int>=24 AND p.min_deadline::int<36 THEN '2年-3年'
             WHEN m4.dict_nm='月' AND p.min_deadline::int>=36 AND p.min_deadline::int<60 THEN '3年-5年'
             WHEN m4.dict_nm='月' AND p.min_deadline::int>=60 THEN '5年及以上'
             ELSE '1年以内' END invst_term_tag,
        CASE WHEN p.open_all_mgr='1' THEN 'Y' ELSE 'N' END  AS open_all_fp, -- 是否开放所有理财师
        'pf' src_sys_cd
    FROM ods.pf_prod_expi_date p
    LEFT JOIN ods.pf_prod_info m7 ON p.prod_id=m7.id
    LEFT JOIN tp_d ON p.id=tp_d.shr_type_id
--     INNER JOIN dd.src_sys_info i ON i.sys_alia_cd='pf'
    LEFT JOIN edw.dict_src_sys m6 ON m6.sys_alia_cd='pf'
        AND m6.group_cd='PAYMENT_TYPE' AND m6.dict_key=p.pay_type
    LEFT JOIN edw.dict_src_sys m5 ON m5.sys_alia_cd='pf'
        AND m5.group_cd='INTEREST_TYPE' AND m5.dict_key=p.prof_calc_type
    LEFT JOIN edw.dict_src_sys m4 ON m4.sys_alia_cd='pf'
        AND m4.group_cd='DEADLINE_UNIT' AND m4.dict_key=p.deadline_unit
    WHERE m7.rec_stat='1' AND p.rec_stat='1' and p.id>0;