truncate table edw.fact_order_pf;
insert into edw.fact_order_pf (order_cd, trans_cd, --  交易编号
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
                               orgn_crcy_amnt ,    --  原币金额
                               order_crt_dtm
    )
SELECT 'pf_'||a.id :: varchar                                               as order_cd,
       a.req_seq                                                            AS trans_cd,
       e.cont_no                                                            AS cont_cd,
       'pf'                                                                 AS src_sys_cd,
       'pf_'||a.prod_id :: varchar                                          AS prod_id,
       'pf_'||b.id                                                          AS prod_shr_type_id,
       'pf_'||acct.cust_id                                                  As accnt_cd,
       a.mgr_code                                                           AS emp_cd,
       COALESCE(a.brah_id, 709)                                             AS comp_id,
       dm.cust_no                                                           AS cust_no,
       a.remit_date                                                         AS cont_start_dt,
       COALESCE(a.cont_end_date,b.real_expi_date)                           AS cont_end_dt,
       a.remit_date                                                         AS intst_start_dt, -- 计息开始
       coalesce(a.comt_date, a.create_time)                                 AS entry_dt,
       case
         when a.comt_date is not null
                 then to_timestamp(to_char(a.comt_date, 'YYYY-MM-DD') || ' ' || COALESCE(a.comt_time, '000000'), 'YYYY-MM-DD hh24miss')
         else a.create_time end                                             AS entry_dtm,
       a.req_amt :: decimal                                                 AS cont_amnt,
       a.conf_stat                                                          AS conf_stat_cd,
       m1.dict_nm                                                            as conf_stat_nm,
       a.req_stat                                                           AS req_stat_cd,
       m2.dict_nm                                                            as req_stat_nm,
       CASE WHEN a.conf_stat IN ('3', '7', '8', '13') THEN 'Y' ELSE 'N' END AS conf_stat_flag,
       case
         when a.trad_code IN ('0020', '0022')     -- 认购，申购
                 then 'Y'
         else 'N' end                                                       as is_buy,
       shr.std_kpi                                                           as norm_std_kpi,
       a.trad_code                                                          AS trad_cd,
       m3.dict_nm                                                            as trad_nm,
       CASE
         WHEN a.conf_stat IN ('3', '7', '8') THEN g.conf_time_07
         WHEN a.conf_stat = '13' THEN g.conf_time_37
           END                                                              AS conf_dtm,
       a.curr_type                                                          AS crcy_type,      -- 币种
       a.req_amt                                                            AS orgn_crcy_amnt, -- 原币金额
       a.create_time order_crt_dtm
FROM ods.pf_cust_trad_req a
       left join ods.pf_trad_acct_info acct on a.trad_acct = acct.trad_acct
       LEFT JOIN ods.pf_prod_expi_date b ON a.prod_id = b.prod_id AND a.shr_type_id = b.id
       LEFT JOIN ods.pf_prod_info c ON a.prod_id = c.id
       LEFT JOIN ods.pf_cont_ser_no e ON a.order_cont_no = e.order_cont_no and e.rec_stat='1'
       LEFT JOIN (
                 -- 总审通过时间
                 SELECT req_id,
                        max(CASE WHEN oper_type = '07' THEN create_time END) AS conf_time_07,
                        max(CASE WHEN oper_type = '37' THEN create_time END) AS conf_time_37
                 FROM ods.pf_trad_req_log
                 WHERE oper_type IN ('07', '37')
                   AND chk_stat = '1'
                   AND rec_stat = '1'
                 GROUP BY req_id) g ON a.req_seq = g.req_id
       left join edw.dict_src_sys m1 on m1.group_cd = 'CONF_STAT' and m1.sys_alia_cd = 'pf' and a.conf_stat = m1.dict_key
       left join edw.dict_src_sys m2 on m2.group_cd = 'REQ_STAT' and m2.sys_alia_cd = 'pf' and a.rec_stat = m2.dict_key
       left join edw.dict_src_sys m3 on m3.group_cd = 'TRAD_CODE' and m3.sys_alia_cd = 'pf' and a.trad_code = m3.dict_key
       left join edw.dim_accnt dm on dm.accnt_cd = 'pf_'||acct.cust_id
       left join edw.dim_prod_shr_type shr on a.shr_type_id = shr.src_shr_type_id :: int and shr.src_sys_cd = 'pf'
WHERE a.rec_stat = '1'        -- 1-有效
  AND b.rec_stat = '1'
  AND c.rec_stat = '1'
  and acct.rec_stat = '1'
  and coalesce(a.comt_date, a.create_time) > '2000-01-01'; -- 排除null 值