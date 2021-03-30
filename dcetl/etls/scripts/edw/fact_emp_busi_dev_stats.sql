delete from edw.fact_emp_busi_dev_stats where oper_dt >=current_date-${p_days} ;
insert into edw.fact_emp_busi_dev_stats(emp_cd,oper_dt,leads_cnt,shr_cnt,brs_cnt,frwrd_cnt,vstr_cnt,chnl_cd)
with share as(
    select s.card_id,
    s.share_time::date dt,
    count(1) shr_cnt 
    from  ${schm}.portal_wechat_card_share s where s.share_time>=current_date-${p_days}
    group by s.card_id,s.share_time::date
    )
,brs as(
    select l.card_id,l.create_time::date dt,
    sum(case when l."type"='0' then 1 else 0 end) brs_cnt,
    sum(case when l."type"='3' then 1 else 0 end) frwrd_cnt,
    count(distinct l.visitor_id) vstr_cnt
    from ${schm}.portal_wechat_card_log l
    -- inner join ${schm}.portal_wechat_card_share s on l.share_id=s.id and  s.status='0'
    where l.type in ('0' ,'3')
    and l.create_time>=current_date-${p_days}
    group by l.card_id,l.create_time::date)
,ma as(
    select card_id,dt from share
    union 
    select card_id,dt from brs
),
leads as(
    select emp_cd,
    leads_crt_dtm::date dt, 
    count(1) leads_cnt 
    from edw.fact_cust_leads 
    where chnl_cd='wx_card' and leads_crt_dtm >=current_date-${p_days}
    group by emp_cd,leads_crt_dtm::date
    )
select 
    c.user_no emp_cd,
    ma.dt oper_dt,
    coalesce(leads.leads_cnt,0)  leads_cnt,
    coalesce(share.shr_cnt,0)  shr_cnt,
    coalesce(brs.brs_cnt,0) brs_cnt,
    coalesce(brs.frwrd_cnt,0) frwrd_cnt,
    coalesce(brs.vstr_cnt,0) vstr_cnt,
    'wx_card'::varchar chnl_cd
from ma 
left join share on ma.dt=share.dt and ma.card_id=share.card_id
left join brs on ma.dt=brs.dt and ma.card_id=brs.card_id
left join ${schm}.portal_wechat_card c on c.id=ma.card_id
left join leads on ma.dt=leads.dt and c.user_no=leads.emp_cd where c.user_no>'';
-- 小程序 空中课堂逻辑
insert into edw.fact_emp_busi_dev_stats(emp_cd,oper_dt,leads_cnt,shr_cnt,brs_cnt,frwrd_cnt,vstr_cnt,chnl_cd)
with tp_sh as( -- 父子share明细
    select id share_id,
    case when s.mgm_seq is null then s.id else  split_part(s.mgm_seq,'.',1)::int end  as root_share_id
    from ${schm}.cms_mp_act_share s where   status='0'),
tp_share as ( -- 分享id对应的理财师
    select
    tp_sh.share_id,
    s.share_no emp_cd
    from tp_sh
    inner join ${schm}.cms_mp_act_share s
    on tp_sh.root_share_id=s.id and s.user_type='0' and s.status='0'),
share as(  -- 理财师分享数
    select s.share_no emp_cd,
    s.share_time::date dt,
    count(1) shr_cnt
    from  ${schm}.cms_mp_act_share s where s.share_time>=current_date-${p_days} and status='0' and user_type='0'
    group by s.share_no,s.share_time::date
    ),
frwrd as(  -- 转发汇总
    select
    tp_share.emp_cd,t.share_time::date dt,count(1) frwrd_cnt
    from ${schm}.cms_mp_act_share t inner join tp_share on tp_share.share_id=t.id
    where t.status='0' and t.share_time>=current_date-${p_days} and t.user_type='2' and t.parent_id>0 and t.mgm_seq>''
    -- 必须是二次转发 所以parent_id大于0 user_type=0 是理财师 user_type=2 是客户
    group by tp_share.emp_cd,t.share_time::date),
brs as( -- 浏览客户数和浏览次数
    select
    tp_share.emp_cd,
    l.create_time::date dt,
    count(1) brs_cnt,
    count(distinct l.account_id) vstr_cnt
    from ${schm}.cms_mp_act_log l
    inner join tp_share on l.create_time>=current_date-${p_days} and l.share_id=tp_share.share_id
    group by tp_share.emp_cd,l.create_time::date),
leads as(  -- 新增有效线索客户数
    select emp_cd,
    leads_crt_dtm::date dt,
    count(1) leads_cnt
    from edw.fact_cust_leads
    where chnl_cd='wx_mp' and leads_crt_dtm >=current_date-${p_days}
    group by emp_cd,leads_crt_dtm::date
    )
,ma as(
    select emp_cd,dt,0 leads_cnt,  shr_cnt,0 brs_cnt, 0 frwrd_cnt, 0 vstr_cnt from share
      union all
    select emp_cd,dt,0 leads_cnt,0 shr_cnt,0 brs_cnt,   frwrd_cnt, 0 vstr_cnt from frwrd
      union all
    select emp_cd,dt,0 leads_cnt,0 shr_cnt,  brs_cnt, 0 frwrd_cnt,   vstr_cnt from brs
      union all
    select emp_cd,dt,  leads_cnt,0 shr_cnt,0 brs_cnt, 0 frwrd_cnt, 0 vstr_cnt from leads
)
select
    ma.emp_cd,
    ma.dt oper_dt,
    sum(leads_cnt)  leads_cnt,
    sum(shr_cnt)  shr_cnt,
    sum(brs_cnt) brs_cnt,
    sum(frwrd_cnt) frwrd_cnt,
    sum(vstr_cnt) vstr_cnt,
    'wx_mp'::varchar chnl_cd
from ma where dt>=current_date-${p_days} and ma.emp_cd>''
group by emp_cd,dt;