delete from dms.rpt_emp_busi_dev_stats where oper_dt>=current_date-${p_days};
with tp_lead as(
select  emp_cd,
        oper_dt,
        -- share_chnl, , 加入渠道会导致当日的访客数虚增，因为可能一个客户访问了多个渠道的内，暂时不加入渠道
        count(1) leads_cnt,
        0 share_cnt,
        0 frwrd_cnt,
        0 brws_cnt,
        0 vstr_cnt
from edw.fact_busi_dev_leads
  where emp_cd>'' and oper_dt>=current_date-${p_days}
group by emp_cd,oper_dt
  union all
select
  emp_cd,
  oper_dt,
--   share_chnl, , 加入渠道会导致当日的访客数虚增，因为可能一个客户访问了多个渠道的内，暂时不加入渠道
  0 leads_cnt,
  sum(is_share) share_cnt,
  sum(is_frwrd) frwrd_cnt,
  sum(is_brws) brws_cnt,
count(distinct case when is_brws=1 then src_accnt_id else null end) vstr_cnt
from edw.fact_busi_dev_log
  where emp_cd>'' and oper_dt>=current_date-${p_days}
group by emp_cd,oper_dt)
insert into dms.rpt_emp_busi_dev_stats(emp_cd,oper_dt,leads_cnt,share_cnt,frwrd_cnt,brws_cnt,vstr_cnt)
select emp_cd,
       oper_dt,
--        share_chnl, 加入渠道会导致当日的访客数虚增，因为可能一个客户访问了多个渠道的内，暂时不加入渠道
      sum(leads_cnt) leads_cnt,
      sum(share_cnt) share_cnt,
      sum(frwrd_cnt) frwrd_cnt,
      sum(brws_cnt) brws_cnt,
      sum(vstr_cnt) vstr_cnt
from tp_lead group by emp_cd,oper_dt;
