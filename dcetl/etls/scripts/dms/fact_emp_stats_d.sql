-- 新老系统切换，表备份，主要备份2020-07-01之前的老数据，数月后以后可以不再备份，但需要保留该备份。
-- create table if not exists dms.fact_emp_stats_d_bak(like dms.fact_emp_stats_d INCLUDING ALL);
-- insert into  dms.fact_emp_stats_d_bak
-- select * from  dms.fact_emp_stats_d;
-- 更新当前数据
delete from dms.fact_emp_stats_d where stats_dt>=current_date-${p_days} and stats_dt>='2020-07-01';
insert into dms.fact_emp_stats_d(
      emp_cd    ,
      stats_dt  ,
      leads_cnt ,
      shr_cnt   ,
      brs_cnt   ,
      frwrd_cnt ,
      vstr_cnt  ,
      camp_invt_cust_cnt    ,
      camp_cust_arrv_cnt    ,
      order_cnt ,
      order_amnt    ,
      pay_arrv_cust_cnt    ,
      pay_arrv_amnt
    )
with tp_pay as(
 select
    emp_cd,
    pay_arrv_dt dt,
    count(distinct accnt_cd) pay_arrv_cust_cnt,
    sum(pay_arrv_amnt) pay_arrv_amnt
    from (
        select
            mgr_capi_seq_id,
            pay_arrv_dt,
            emp_cd,
            accnt_cd,max(pay_arrv_amnt) pay_arrv_amnt
        from edw.fact_cust_pay where pay_arrv_dt>=current_date-${p_days} and is_valid='Y'
        group by mgr_capi_seq_id,pay_arrv_dt,emp_cd,accnt_cd
    ) m group by emp_cd,pay_arrv_dt
    ),
tp_custdev as(
select
    emp_cd,
    oper_dt dt,
    sum(leads_cnt) leads_cnt,
    sum(share_cnt) shr_cnt,
    sum(brws_cnt) brs_cnt,
    sum(frwrd_cnt) frwrd_cnt,
    sum(vstr_cnt) vstr_cnt
from dms.rpt_emp_busi_dev_stats where oper_dt>=current_date-${p_days}
 group by emp_cd,oper_dt),
tp_camp as(
     select
        emp_cd,
        camp_start_dt dt,
        count(distinct accnt_cd) camp_invt_cust_cnt,
        sum(arrv_flag) camp_cust_arrv_cnt
    from edw.fact_cust_camp_order
    where camp_start_dt>=current_date-${p_days} group by emp_cd,camp_start_dt),
tp_order as(
    select emp_cd,order_dtm::date dt,
    count(distinct accnt_cd) order_cnt,
    sum(order_amnt) order_amnt
    from edw.fact_pre_order
    where is_valid_order='Y' and order_dtm>=current_date-${p_days}
    group by emp_cd,order_dtm::date),
tp_emp as(
 select emp_cd,dt from tp_pay where emp_cd>''
 union
  select emp_cd,dt from tp_custdev  where emp_cd>''
 union
  select emp_cd,dt from tp_camp  where emp_cd>''
 union
  select emp_cd,dt from tp_order  where emp_cd>''
 )
select
  t.emp_cd,
  t.dt stats_dt,
  coalesce(tp_custdev.leads_cnt,0) leads_cnt, -- 新增销售线索数
  coalesce(tp_custdev.shr_cnt,0) shr_cnt,  -- 分享次数
  coalesce(tp_custdev.brs_cnt,0) brs_cnt,  -- 浏览量
  coalesce(tp_custdev.frwrd_cnt,0) frwrd_cnt,  -- 被转发次数
  coalesce(tp_custdev.vstr_cnt,0) vstr_cnt, -- 客户数 去重了的
  coalesce(tp_camp.camp_invt_cust_cnt,0) camp_invt_cust_cnt,
  coalesce(tp_camp.camp_cust_arrv_cnt,0) camp_cust_arrv_cnt,
  coalesce(tp_order.order_cnt,0) order_cnt,
  coalesce(tp_order.order_amnt,0) order_amnt,
  coalesce(tp_pay.pay_arrv_cust_cnt,0) pay_arrv_cust_cnt,
  coalesce(tp_pay.pay_arrv_amnt,0) pay_arrv_amnt
from tp_emp t
left join tp_pay on t.emp_cd=tp_pay.emp_cd and t.dt=tp_pay.dt
left join tp_custdev on t.emp_cd=tp_custdev.emp_cd and t.dt=tp_custdev.dt
left join tp_camp on t.emp_cd=tp_camp.emp_cd and t.dt=tp_camp.dt
left join tp_order on t.emp_cd=tp_order.emp_cd and t.dt=tp_order.dt
where t.dt>='2020-07-01' and  -- 新客拓展上线时间
 t.emp_cd>'';