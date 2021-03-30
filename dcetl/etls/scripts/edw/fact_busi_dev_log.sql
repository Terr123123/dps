delete from edw.fact_busi_dev_log where crt_dtm>=current_date-${p_days};
insert into edw.fact_busi_dev_log (id,
                                   parent_id,
                                   emp_cd,
                                   crt_dtm,
                                   oper_dt,
                                   src_accnt_id,
                                   drnt_time,
                                   info_id,
                                   info_nm,
                                   share_id,
                                   share_chnl,
                                   action_type,
                                   is_share,
                                   is_frwrd,
                                   is_brws)
select id,
       a.parent_id,
       owner_id                                                                            emp_cd,
       a.time_created                                                                      crt_dtm,
       a.time_created :: date                                                              oper_dt,
       receiver_id                                                                         src_accnt_id,
       a.time_duration                                                                     drnt_time,
        case when a.respondent_realm='MICROMARKETING' then a.owner_id
             else a.respondent_id end info_id,
       a.title                                                                             info_nm,
       a.respondent_id                                                                     share_id,
      lower(a.respondent_realm)                                                            share_chnl,
       a."type"                                                                            action_type,
       case
         when a.type IN ('SHARE_MOMENTS', 'SHARE', 'SHARE_CHAT') and a.receiver_realm = 'USER' then 1
         else 0 end                                                                        is_share,
       case
         when a.type IN ('SHARE_MOMENTS', 'SHARE', 'SHARE_CHAT') and a.receiver_realm = 'WECHAT_INFO' then 1
         else 0 end                                                                        is_frward,
       case when a.type IN ('VIEW') and a.receiver_realm = 'WECHAT_INFO' then 1 else 0 end is_brws
from ${schm}.busi_dev_activity a
where group_id = '8008' and  a.time_created>=current_date-${p_days};
-- 小程序分享和转发数据
insert into edw.fact_busi_dev_log (id,
                                   parent_id,
                                   emp_cd,
                                   crt_dtm,
                                   oper_dt,
                                   src_accnt_id,
                                   drnt_time,
                                   info_id,
                                   info_nm,
                                   share_id,
                                   share_chnl,
                                   action_type,
                                   is_share,
                                   is_frwrd,
                                   is_brws)
select 'mp_'||s.id :: varchar  id, -- 通过小程序分享
       'mp_'||parent_id        parent_id,
       owner.emp_cd,
       s.share_time            crt_dtm,
       s.share_time :: date    oper_dt,
       s.account_id :: varchar src_accnt_id,
       null                    drnt_time,
       owner.info_id,
       owner.info_nm           share_title,
       'mp_'||split_part(s.mgm_seq, '.', 1)       share_id,
       'mp' as share_chnl,
       null                    action_type,
       case when user_type = '0' and s.parent_id is null and owner.emp_cd > '' then 1 else 0 end      is_share,
       case when user_type <> '0' and s.parent_id > 0 and owner.emp_cd > '' then 1 else 0 end     is_frward,
       0                       is_brws
from ${schm}.cms_mp_act_share s
       left join stg.stg_busi_dev_share owner on 'mp_'||split_part(s.mgm_seq, '.', 1) = owner.share_id
where status = '0' and  s.share_time>=current_date-${p_days};
-- 小程序浏览数据
insert into edw.fact_busi_dev_log (id,
                                   parent_id,
                                   emp_cd,
                                   crt_dtm,
                                   oper_dt,
                                   src_accnt_id,
                                   drnt_time,
                                   info_id,
                                   info_nm,
                                   share_id,
                                   share_chnl,
                                   action_type,
                                   is_share,
                                   is_frwrd,
                                   is_brws)
select 'mp_'||s.id :: varchar id,
       'mp_'||ss.parent_id    parent_id,
       r.emp_cd,
       s.create_time          oper_dtm,
       s.create_time :: date  oper_dt,
       s.account_id           src_accnt_id,
       s.duration             drnt_time,
       r.info_id,
       r.info_nm              share_title,
       r.share_id share_id,
       'mp' as share_chnl,
       null as                action_type,
       0                      is_share,
       0                      is_frward,
       1                      is_brws
from ${schm}.cms_mp_act_log s
       inner join ods.cms_mp_act_share ss on s.share_id = ss.id and ss.status = '0'
       left join stg.stg_busi_dev_share r on 'mp_'||split_part(ss.mgm_seq, '.', 1) = r.share_id
where s.share_id > 0 and  s.create_time>=current_date-${p_days};