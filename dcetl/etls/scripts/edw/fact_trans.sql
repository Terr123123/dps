delete from edw.fact_trans where order_crt_dtm >= current_date - ${p_days};
insert into edw.fact_trans (order_cd, --  订单编号
                            trans_cd, --  交易编号
                            cont_cd, --  合同编号
                            src_sys_cd, --  源系统编码
                            prod_id, --  产品编号
                            prod_shr_type_id, --  份额类别
                            prod_open_id, --  归属开放期ID
                            accnt_cd, --  交易账号
                            emp_cd, --  理财师工号
                            comp_id, --  分公司编号
                            cust_no, --  客户编号
                            cont_start_dt, --  合同开始日期
                            cont_end_dt, --  合同结束日期
                            intst_start_dt, --  起息日期
                            entry_dt, --  录入日期
                            entry_dtm, --  录入时间
                            cont_amnt, --  合同金额
                            conf_stat_cd, --  审核状态编号
                            conf_stat_nm, --  审核状态名称
                            req_stat_cd, --  申请状态编号
                            req_stat_nm, --  申请状态名称
                            trans_dt, --  交易日期
                            conf_stat_flag, --  确认标识
                            is_hold, --  是否持有，用来计算当前持有资产
                            is_in_transt, -- 在途资金
                            is_cncl,
                            norm_std_kpi, --  常规kpi系数
                            trans_type_cd, --  交易类型编号
                            trans_type_nm, --  交易类型名称
                            conf_dtm, --  总审通过时间
                            crcy_type, --  币种
                            orgn_crcy_amnt,     --  原币金额
                            order_crt_dtm
    )
select order_cd,                  --  订单编号
       p.trans_cd,                --  交易编号
       p.cont_cd,                 --  合同编号
       p.src_sys_cd,              --  源系统编码
       p.prod_id,                 --  产品编号
       p.prod_shr_type_id,        --  份额类别
       p.prod_open_id,            --  归属开放期ID
       p.accnt_cd,                --  交易账号
       p.emp_cd,                  --  理财师工号
       p.comp_id,                 --  分公司编号
       p.cust_no,                 --  客户编号
       p.cont_start_dt,           --  合同开始日期
       p.cont_end_dt,             --  合同结束日期
       p.intst_start_dt,          --  起息日期
       p.entry_dt,                --  录入日期
       p.entry_dtm,               --  录入时间
       p.cont_amnt,               --  合同金额
       p.conf_stat_cd,            --  审核状态编号
       p.conf_stat_nm,            --  审核状态名称
       p.req_stat_cd,             --  申请状态编号
       p.req_stat_nm,             --  申请状态名称
       p.entry_dt trans_dt,       --  交易日期
       p.conf_stat_flag,          --  确认标识
       case when  conf_stat_flag='Y' and p.req_stat_cd='4'
              and (current_date between coalesce(p.cont_start_dt,p.intst_start_dt) and coalesce(p.cont_end_dt,current_date)) then 'Y'
           else 'N' end is_hold,              --  是否持有，用来计算当前持有资产  逻辑待确认
       case when conf_stat_flag='Y' and req_stat_cd in('1','2','3') then 'Y' else 'N' end is_in_transt,
       case when p.req_stat_cd  IN ('5', '6')     -- 5-确认失败, 6-已撤销
                and p.conf_stat_cd  IN ('0', '6')  -- 复核状态:非待提交,非作废
            then 'Y' else  'N' end is_cncl,
       s.std_kpi  norm_std_kpi,   --  常规kpi系数
       p.trans_type_cd,           --  交易类型编号
       p.trans_type_nm,           --  交易类型名称
       p.conf_dtm,                --  总审通过时间
       p.crcy_type,               --  币种
       p.orgn_crcy_amnt,     --  原币金额
       p.order_crt_dtm
from edw.fact_order_pf p
       left join edw.dim_prod_shr_type s on p.prod_shr_type_id = s.prod_shr_type_id and p.prod_id = s.prod_id
where p.is_buy = 'Y' and  p.order_crt_dtm >= current_date - ${p_days};