truncate table edw.fact_order_bx;
insert into edw.fact_order_bx (order_cd, -- 订单编号
                               trans_cd, --  交易编号
                               cont_cd, --  合同编号
                               src_sys_cd, --  源系统编码
                               prod_id, --
                               prod_shr_type_id, --  份额类别
                               accnt_cd, --  账户编号
                               emp_cd, --  理财师工号
                               comp_id, --  分公司编号
                               cust_no, --  客户编号
                               cont_start_dt, --  合同开始日期
                               cont_end_dt, --  合同结束日期
                               intst_start_dt, --  起息日期
                               entry_dt, --
                               entry_dtm, --
                               cont_amnt, --  合同金额
                               conf_stat_cd, --  审核状态编号
                               conf_stat_nm, --  审核状态名称
                               req_stat_cd, --  申请状态编号
                               req_stat_nm, --  申请状态名称
                               conf_stat_flag, --  确认标识
                               is_buy, --  是否买入
                               norm_std_kpi, -- 常规KPI
                               trans_type_cd, --  交易类型编号
                               trans_type_nm, --  交易类型名称
                               conf_dtm, --  总审通过时间
                               crcy_type, --  币种
                               orgn_crcy_amnt     --  原币金额
    )
select 'bx_'||a.id :: varchar                                        as order_cd,
       a.id                                                          AS trans_cd,
       trim(a.cont_no)                                               AS cont_cd,
       'bx'                                                          AS src_sys_cd,
       'pf_'||a.prod_id :: varchar                                   AS prod_id,
       null                                                          as prod_shr_type_id,
       'pf_'||a.cust_id                                              As accnt_cd,
       COALESCE(b.mgr_code, a.mgr_code)                              AS emp_cd,
       COALESCE(b.brah_id, a.brah_id, 709)                           AS comp_id,
       dm.cust_no                                                    AS cust_no,
       a.effect_date                                                 AS cont_start_dt,
       NULL                                                          AS cont_end_dt,
       a.effect_date                                                 AS intst_start_dt,
       coalesce(c.create_time, a.create_time)                        AS entry_dt,
       coalesce(c.create_time, a.create_time)                        AS entry_dtm,
       a.premium_yrmb                                                AS cont_amnt,
       a.conf_stat                                                   AS conf_stat_cd,
       m2.dic_nm                                                     as conf_stat_nm,
       a.insure_stat                                                 AS req_stat_cd,
       m1.dic_nm                                                     as req_stat_nm,
       CASE WHEN a.conf_stat IN ('2') THEN 'Y' ELSE 'N' END          AS conf_stat_flag,
       CASE when a.insure_stat IN ('01', '05') then 'Y' else 'N' end AS is_buy,         -- 01-生效,05-承保
       CASE WHEN a.pay_term >= 15 THEN 15 ELSE a.pay_term END        AS norm_std_kpi,
       '02'                                                          AS trans_type_cd,
       '承保'                                                          as trans_type_nm,
       a.effect_date                                                 AS conf_dtm,
       a.pay_cur_type                                                AS crcy_type,      -- 币种
       a.premium_year                                                AS orgn_crcy_amt   -- 原币金额
FROM prifund.hpf_insu_cont a
       LEFT JOIN prifund.hpf_insu_trans b ON a.id = b.insure_id AND b.oper_type = '05' -- 05承保
       LEFT JOIN prifund.hpf_insu_appr c ON a.id = c.insure_id AND c.oper_type = '02' -- 02录入
       LEFT JOIN prifund.hpf_safe_prod e ON a.prod_id = e.id
       LEFT JOIN prifund.cust_info f ON a.cust_id = f.id
       left join edw.dim_accnt dm on dm.accnt_cd = 'pf_'||a.cust_id
       left join dd.src_sys_dic_map m1 on m1.group_cd = 'INSURE_STAT' and m1.sys_alia_cd = 'pf' and a.insure_stat = m1.dic_key
       left join dd.src_sys_dic_map m2 on m2.group_cd = 'INSURE_CONF_STAT' and m2.sys_alia_cd = 'pf' and a.conf_stat = m2.dic_key
WHERE a.rec_stat = '1'
  and c.create_time < '2019-04-01';
-- 2019年4月1日后数据处理
insert into edw.fact_order_bx (order_cd, -- 订单编号
                               trans_cd, --  交易编号
                               cont_cd, --  合同编号
                               src_sys_cd, --  源系统编码
                               prod_id, --
                               prod_shr_type_id, --  份额类别
                               accnt_cd, --  账户编号
                               emp_cd, --  理财师工号
                               comp_id, --  分公司编号
                               cust_no, --  客户编号
                               cont_start_dt, --  合同开始日期
                               cont_end_dt, --  合同结束日期
                               intst_start_dt, --  起息日期
                               entry_dt, --
                               entry_dtm, --
                               cont_amnt, --  合同金额
                               conf_stat_cd, --  审核状态编号
                               conf_stat_nm, --  审核状态名称
                               req_stat_cd, --  申请状态编号
                               req_stat_nm, --  申请状态名称
                               conf_stat_flag, --  确认标识
                               is_buy, --  是否买入
                               norm_std_kpi, -- 常规KPI
                               trans_type_cd, --  交易类型编号
                               trans_type_nm, --  交易类型名称
                               conf_dtm, --  总审通过时间
                               crcy_type, --  币种
                               orgn_crcy_amnt     --  原币金额
    )
with conf as (select insure_id, min(create_time) conf_dtm
              from prifund.hpf_insu_appr c
              where c.oper_type = '06'
                and create_time >= '2019-04-01'
              group by insure_id),
     oper as (select c.insure_id,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        -- 保单id
                     min(create_time)    oper_dtm,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 -- 操作时间
                     c.oper_type,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               -- 操作类型
                     max(c.premium_yrmb) premium_yrmb -- 金额
              from prifund.hpf_insu_appr c
              where c.oper_type in ('18', '12', '14', '16', '17', '02')
                and c.create_time >= '2019-04-01'
              group by c.insure_id, c.oper_type)
select 'bx_'||a.id :: varchar                          as order_cd,
       a.id                                                           AS trans_cd,
       trim(a.cont_no)                                                AS cont_cd,
       'bx'                                                           AS src_sys_cd,
       'pf_'||a.prod_id :: varchar                                    AS prod_id,
       null                                                           as prod_shr_type_id,
       'pf_'||a.cust_id                                               As accnt_cd,
       COALESCE(a.mgr_code, b.mgr_code)                               AS emp_cd,
       COALESCE(a.brah_id, 709)                                       AS comp_id,
       dm.cust_no                                                     AS cust_no,
       a.effect_date                                                  AS cont_start_dt,
       NULL                                                           AS cont_end_dt,
       a.effect_date                                                  AS intst_start_dt,
       coalesce(oper.oper_dtm, a.create_time)                         AS entry_dt,
       coalesce(oper.oper_dtm, a.create_time)                         AS entry_dtm,
       case
         when oper.oper_type = '02' then c.premium_yrmb
         when oper.oper_type = '18' then oper.premium_yrmb - c.premium_yrmb
         else 0 - oper.premium_yrmb end                               AS cont_amnt,      -- 取录入金额
       a.conf_stat                                                    AS conf_stat_cd,
       m2.dic_nm                                                      as conf_stat_nm,
       a.insure_stat                                                  AS req_stat_cd,
       m1.dic_nm                                                      as req_stat_nm,
       CASE WHEN a.conf_stat IN ('2') THEN 'Y' ELSE 'N' END           AS conf_stat_flag,
       CASE
         when a.insure_stat IN ('01', '05') and conf.conf_dtm is not null then 'Y'
         else 'N' end                                                 AS is_buy,         -- 01-生效,05-承保
       CASE WHEN a.pay_term >= 15 THEN 15 ELSE a.pay_term END         AS norm_std_kpi,
       oper.oper_type                                                 AS trans_type_cd,
       case
         when oper.oper_type = '02' then '承保'
         when oper.oper_type = '18' then '减额承保'
         else '退保拒保' end                                              AS trans_type_nm,
       conf.conf_dtm                                                  AS conf_dtm,
       a.pay_cur_type                                                 AS crcy_type,      -- 币种
       case when oper.oper_type = '02' then c.premium_year else 0 end AS orgn_crcy_amt   -- 原币金额
FROM prifund.hpf_insu_cont a
       LEFT JOIN prifund.hpf_insu_trans b ON a.id = b.insure_id AND b.oper_type = '05' -- 05承保
       LEFT JOIN prifund.hpf_insu_appr c ON a.id = c.insure_id AND c.oper_type = '02' -- 02录入
       LEFT JOIN prifund.hpf_safe_prod e ON a.prod_id = e.id
       LEFT JOIN prifund.cust_info f ON a.cust_id = f.id
       INNER JOIN conf on conf.insure_id = a.id
       left join oper on oper.insure_id = a.id
       left join edw.dim_accnt dm on dm.accnt_cd = 'pf_'||a.cust_id
       left join dd.src_sys_dic_map m1 on m1.group_cd = 'INSURE_STAT' and m1.sys_alia_cd = 'pf' and a.insure_stat = m1.dic_key
       left join dd.src_sys_dic_map m2 on m2.group_cd = 'INSURE_CONF_STAT' and m2.sys_alia_cd = 'pf' and a.conf_stat = m2.dic_key
WHERE c.create_time >= '2019-04-01';
