-- -- 微信公众号 微信名片等
-- insert into edw.fact_busi_dev_leads (emp_cd, wx_id, accnt_cd, leads_crt_dtm, chnl_cd)
-- with a as(
--      -- 先获取增量处理的数据
--   select
--   c.user_no emp_cd,
--   v.wx_id,
--   v.cust_no crm_cust_cd,
--   v.create_time
--   from {schm}.portal_wechat_card_visitor v
--   left join {schm}.portal_wechat_card c on v.card_id=c.id
--   where v.mobile>'' and v.create_time>=current_date-${p_days} and v.wx_id>''
--     ),
-- m as(
--      -- 查找客户信息 查找符合 条件的数据
--   select
--   a.emp_cd,
--   a.wx_id,
--   a.crm_cust_cd accnt_cd, -- 账户信息维度表上线后需要更新为账户编号
--   coalesce(cm.create_time,a.create_time) crt_dtm,
--   row_number() over(partition by wx_id order by coalesce(cm.create_time,a.create_time)) rk
--   from a
--   left join {schm}.crm_hy_cust_info cm on cm.old_custid=a.crm_cust_cd  -- 为了兼容实时的
--   -- left join edw.dim_accnt ac on ac.src_sys_cd='crm' and a.crm_cust_cd=ac.src_sys_cust_cd
--   where (cm.create_time is null or cm.owner_code is null or a.emp_cd=cm.owner_code)
--   and coalesce(cm.create_time,a.create_time)>=current_date-${p_days} and a.wx_id>' '
--   ),
-- rs as(
--      -- 按时间排序选出第一条的数据
--   select
--   m.emp_cd,
--   m.wx_id uuid,
--   m.accnt_cd,
--   m.crt_dtm leads_crt_dtm,
--   'wx_card' chnl_cd
--   from m where m.rk=1 and m.wx_id>'')
-- -- 如果线索表不存在则插入到线索表
-- select rs.emp_cd, rs.uuid, rs.accnt_cd, rs.leads_crt_dtm, rs.chnl_cd
-- from rs
--        left join edw.fact_busi_dev_leads l1 on rs.uuid = l1.wx_id
--        left join edw.fact_busi_dev_leads l2 on rs.accnt_cd = l2.accnt_cd
-- where l1.wx_id is null
--   and l2.wx_id is null;
--
--
-- -- 微信小程序 空中课堂 V直播
-- insert into edw.fact_busi_dev_leads (emp_cd, wx_id, accnt_cd, leads_crt_dtm, chnl_cd)
-- with tp_inc as(
--     select
--         l.cust_no,
--         l.create_time leads_crt_dtm,
--         s.share_no emp_cd,
--         a.open_id uuid,
--         l.channel_code chnl_cd
--     from {schm}.cms_mp_act_customer_log l
--     inner join {schm}.cms_mp_act_share s on l.share_id=s.id and user_type='0' and s.status='0'
--     inner join {schm}.cms_mp_account a on l.cust_no=a.cust_no
--     where l.create_time>current_date-${p_days} and a.open_id >'') ,
-- rs as(
--   select
--     tp_inc.cust_no accnt_cd,
--     tp_inc.leads_crt_dtm,
--     tp_inc.emp_cd,
--     'wx_mp' chnl_cd,
--     tp_inc.uuid
-- from tp_inc
-- left join {schm}.crm_hy_cust_info cm on cm.old_custid=tp_inc.cust_no
-- where cm.owner_code is null or tp_inc.emp_cd=cm.owner_code)
-- select rs.emp_cd, rs.uuid, rs.accnt_cd, rs.leads_crt_dtm, rs.chnl_cd
-- from rs
--        left join edw.fact_busi_dev_leads l1 on rs.uuid = l1.wx_id
--        left join edw.fact_busi_dev_leads l2 on rs.accnt_cd = l2.accnt_cd
-- where l1.wx_id is null and
--    l2.wx_id is null;
 -- 牛头帮开发的展业系统
delete  from  edw.fact_busi_dev_leads where crt_dtm>=current_date-${p_days};
insert into edw.fact_busi_dev_leads (src_accnt_id, emp_cd, crt_dtm, oper_dt, share_id,share_chnl)
SELECT channel_id src_accnt_id,
       owner_id emp_cd,
       time_created crt_dtm,
       time_created :: date oper_dt,
       respondent_id share_id,
       lower(respondent_realm) share_chnl
FROM ${schm}.busi_dev_investor_acquisition a
WHERE a.group_id = '8008' and time_created>=current_date-${p_days}
  and a.channel_id>''
  AND a.status = 'INITIALIZED' and owner_id>'';
-- 小程序线索
insert into edw.fact_busi_dev_leads (src_accnt_id, emp_cd, crt_dtm, oper_dt, share_id,share_chnl)
select a.src_accnt_id,
       s.emp_cd,
       l.create_time crt_dtm,
       l.create_time :: date oper_dt,
       s.id share_id,
       'mp' as share_chnl
from ${schm}.cms_mp_act_customer_log l
       inner join edw.fact_busi_dev_log s on 'mp_'||l.share_id = s.id
       inner join edw.dim_busi_dev_user a on l.cust_no = a.crm_cust_no and a.user_type = 'mp'
where   l.create_time>=current_date-${p_days} and a.src_accnt_id>'' and s.emp_cd>'';