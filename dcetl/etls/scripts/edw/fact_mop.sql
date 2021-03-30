truncate table edw.fact_mop;
insert into edw.fact_mop (order_cd, -- 订单编号
                          trans_cd, -- 交易编号
                          cont_cd, -- 合同编号
                          src_sys_cd, -- 源系统编码
                          prod_id, -- 产品编号
                          prod_shr_type_id, -- 份额类别
                          prod_open_id, -- 归属开放期ID
                          accnt_cd, -- 账户编号
                          emp_cd, -- 理财师工号
                          comp_id, -- 分公司编号
                          cust_no, -- 客户编号
                          cont_start_dt, -- 合同开始日期
                          cont_end_dt, -- 合同结束日期
                          intst_start_dt, -- 起息日期
                          entry_dt, -- 录入日期
                          entry_dtm, -- 录入时间
                          cont_amnt, -- 合同金额
                          conf_stat_cd, -- 审核状态编号
                          conf_stat_nm, -- 审核状态名称
                          req_stat_cd, -- 申请状态编号
                          req_stat_nm, -- 申请状态名称
                          trans_dt, -- 交易日期
                          conf_stat_flag, -- 确认标识
                          norm_std_kpi, -- 常规KPI系数
                          trans_type_cd, -- 交易类型编号
                          trans_type_nm, -- 交易类型名称
                          conf_dtm, -- 总审通过时间
                          crcy_type, -- 原币编号
                          orgn_crcy_amnt, -- 原币金额
                          order_crt_dtm, -- 订单创建时间
                          trans_type, -- 交易类型    --  1-交易,   2-保有,   3-延期,   4-红冲,   99-其它
                          std_kpi, -- KPI系数
                          std_amnt    -- 业绩金额
    )
select t.order_cd,-- 订单编号
       t.trans_cd,-- 交易编号
       t.cont_cd,-- 合同编号
       t.src_sys_cd,-- 源系统编码
       t.prod_id,-- 产品编号
       t.prod_shr_type_id,-- 份额类别
       t.prod_open_id,-- 归属开放期ID
       t.accnt_cd,-- 账户编号
       t.emp_cd,-- 理财师工号
       t.comp_id,-- 分公司编号
       t.cust_no,-- 客户编号
       t.cont_start_dt,-- 合同开始日期
       t.cont_end_dt,-- 合同结束日期
       t.intst_start_dt,-- 起息日期
       t.entry_dt,-- 录入日期
       t.entry_dtm,-- 录入时间
       t.cont_amnt,-- 合同金额
       t.conf_stat_cd,-- 审核状态编号
       t.conf_stat_nm,-- 审核状态名称
       t.req_stat_cd,-- 申请状态编号
       t.req_stat_nm,-- 申请状态名称
       t.trans_dt,-- 交易日期
       t.conf_stat_flag,-- 确认标识
       t.norm_std_kpi,-- 常规KPI系数
       t.trans_type_cd,-- 交易类型编号
       t.trans_type_nm,-- 交易类型名称
       t.conf_dtm,-- 总审通过时间
       t.crcy_type,-- 原币编号
       t.orgn_crcy_amnt,-- 原币金额
       t.order_crt_dtm,-- 订单创建时间
       1                                                     trans_type,-- 交易类型    --  1-交易,   2-保有,   3-延期,   4-红冲,   99-其它
       coalesce(m.adj_std_kpi, t.norm_std_kpi)               std_kpi,-- KPI系数
       coalesce(m.adj_std_kpi, t.norm_std_kpi) * t.cont_amnt std_amnt    -- 业绩金额
from edw.fact_trans t
       left join stg.stg_mop_adj_map m on t.order_cd = m.order_cd and m.adj_std_kpi > 0 and is_cncl='N';

insert into edw.fact_mop (order_cd, -- 订单编号
                          trans_cd, -- 交易编号
                          cont_cd, -- 合同编号
                          src_sys_cd, -- 源系统编码
                          prod_id, -- 产品编号
                          prod_shr_type_id, -- 份额类别
                          prod_open_id, -- 归属开放期ID
                          accnt_cd, -- 账户编号
                          emp_cd, -- 理财师工号
                          comp_id, -- 分公司编号
                          cust_no, -- 客户编号
                          cont_start_dt, -- 合同开始日期
                          cont_end_dt, -- 合同结束日期
                          intst_start_dt, -- 起息日期
                          entry_dt, -- 录入日期
                          entry_dtm, -- 录入时间
                          cont_amnt, -- 合同金额
                          conf_stat_cd, -- 审核状态编号
                          conf_stat_nm, -- 审核状态名称
                          req_stat_cd, -- 申请状态编号
                          req_stat_nm, -- 申请状态名称
                          trans_dt, -- 交易日期
                          conf_stat_flag, -- 确认标识
                          norm_std_kpi, -- 常规KPI系数
                          trans_type_cd, -- 交易类型编号
                          trans_type_nm, -- 交易类型名称
                          conf_dtm, -- 总审通过时间
                          crcy_type, -- 原币编号
                          orgn_crcy_amnt, -- 原币金额
                          order_crt_dtm, -- 订单创建时间
                          trans_type, -- 交易类型    --  1-交易,   2-保有,   3-延期,   4-红冲,   99-其它
                          std_kpi, -- KPI系数
                          std_amnt    -- 业绩金额
    )
select t.order_cd||'_rule_'||m.rule_id,-- 订单编号
       t.trans_cd,-- 交易编号
       t.cont_cd,-- 合同编号
       t.src_sys_cd,-- 源系统编码
       t.prod_id,-- 产品编号
       t.prod_shr_type_id,-- 份额类别
       t.prod_open_id,-- 归属开放期ID
       t.accnt_cd,-- 账户编号
       t.emp_cd,-- 理财师工号
       t.comp_id,-- 分公司编号
       t.cust_no,-- 客户编号
       t.cont_start_dt,-- 合同开始日期
       t.cont_end_dt,-- 合同结束日期
       t.intst_start_dt,-- 起息日期
       m.rule_add_dt entry_dt,-- 录入日期
       m.rule_add_dt entry_dtm,-- 录入时间
       0 as cont_amnt,-- 合同金额
       t.conf_stat_cd,-- 审核状态编号
       t.conf_stat_nm,-- 审核状态名称
       t.req_stat_cd,-- 申请状态编号
       t.req_stat_nm,-- 申请状态名称
       m.rule_add_dt trans_dt,-- 交易日期
       t.conf_stat_flag,-- 确认标识
       t.norm_std_kpi,-- 常规KPI系数
       t.trans_type_cd,-- 交易类型编号
       t.trans_type_nm,-- 交易类型名称
       t.conf_dtm,-- 总审通过时间
       t.crcy_type,-- 原币编号
       t.orgn_crcy_amnt,-- 原币金额
       m.rule_add_dt order_crt_dtm,-- 订单创建时间
       case when m.adj_std_amnt_by_ratio<0 or m.adj_std_amnt< 0   -- 红冲 否则是延期或者补发业绩
                 then   4 else 3 end     trans_type,-- 交易类型    --  1-交易,   2-保有,   3-延期,   4-红冲,   99-其它
       t.norm_std_kpi               std_kpi,-- KPI系数
       m.adj_std_amnt_by_ratio*t.cont_amnt+m.adj_std_amnt std_amnt    -- 业绩金额
from edw.fact_trans t
  inner join stg.stg_mop_adj_map m on t.order_cd = m.order_cd and m.adj_std_kpi = 0 and is_cncl='N';

-- 以下添加保有也业绩 或者把上面的红冲和延期也放在保有里面，然后一起处理进来