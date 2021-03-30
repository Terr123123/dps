delete from dms.fact_comp_stats_d where stats_dt>=current_date-${p_days};
insert into dms.fact_comp_stats_d(
      comp_id    ,
      stats_dt  ,
      leads_cnt ,
      shr_cnt   ,
      brs_cnt   ,
      frwrd_cnt ,
      vstr_cnt  ,
      camp_cnt,
      camp_invt_cust_cnt    ,
      camp_cust_arrv_cnt    ,
      order_cnt ,
      order_amnt    ,
      pay_arrv_cust_cnt    ,
      pay_arrv_amnt
    )
with tp_st as(
 select
    coalesce(e.comp_id,709) comp_id,
    s.stats_dt dt,
    sum(s.leads_cnt) leads_cnt,
    sum(s.shr_cnt) shr_cnt,
    sum(s.brs_cnt) brs_cnt,
    sum(s.frwrd_cnt) frwrd_cnt,
    sum(s.vstr_cnt) vstr_cnt,
    sum(camp_invt_cust_cnt) camp_invt_cust_cnt,
    sum(camp_cust_arrv_cnt) camp_cust_arrv_cnt,
    sum(order_cnt) order_cnt,
    sum(order_amnt) order_amnt,
--     sum(amnt_arrv_cust_cnt) amnt_arrv_cust_cnt,
    sum(pay_arrv_amnt) pay_arrv_amnt,
    sum(pay_arrv_cust_cnt) pay_arrv_cust_cnt
from dms.fact_emp_stats_d s
left join dw.dim_emp e on s.emp_cd=e.emp_cd
 where s.stats_dt>=current_date-${p_days}
group by coalesce(e.comp_id,709),s.stats_dt
    ),
tp_camp as(
select
    lau_comp_id comp_id,
    start_dtm::date dt,
    count(1) camp_cnt
from edw.dim_camp where start_dtm>=current_date-${p_days} and lau_comp_id>0
 group by lau_comp_id,start_dtm::date),
tp as(
 select comp_id,dt from tp_st
 union
  select comp_id,dt from tp_camp
 )
select
  t.comp_id,
  t.dt stats_dt,
  coalesce(tp_st.leads_cnt,0) leads_cnt, -- 新增销售线索数
  coalesce(tp_st.shr_cnt,0) shr_cnt,  -- 分享次数
  coalesce(tp_st.brs_cnt,0) brs_cnt,  -- 浏览量
  coalesce(tp_st.frwrd_cnt,0) frwrd_cnt,  -- 被转发次数
  coalesce(tp_st.vstr_cnt,0) vstr_cnt, -- 客户数 去重了的
  coalesce(tp_camp.camp_cnt,0) camp_cnt,
  coalesce(tp_st.camp_invt_cust_cnt,0) camp_invt_cust_cnt,
  coalesce(tp_st.camp_cust_arrv_cnt,0) camp_cust_arrv_cnt,
  coalesce(tp_st.order_cnt,0) order_cnt,
  coalesce(tp_st.order_amnt,0) order_amnt,
  coalesce(tp_st.pay_arrv_cust_cnt,0) pay_arrv_cust_cnt,
  coalesce(tp_st.pay_arrv_amnt,0) pay_arrv_amnt
from tp t
left join tp_st on t.comp_id=tp_st.comp_id and t.dt=tp_st.dt
left join tp_camp on t.comp_id=tp_camp.comp_id and t.dt=tp_camp.dt
where t.comp_id>0;