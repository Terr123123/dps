delete from stg.stg_busi_dev_share where share_dtm >= current_date - ${p_days};
-- 小程序分享 空中课堂 线下活动等直播,中间表，用于记录
insert into stg.stg_busi_dev_share (share_id, --  分享ID
                                    emp_cd, --  员工编号
                                    share_dtm, --  分享时间
                                    share_chnl, --  分享渠道
                                    info_id, --  资讯ID
                                    info_nm, -- 资讯名称
                                    data_from     --  系统来源
                                    -- share_from_who      --  分享来自谁  fp 表示理财师 emp表示非理财师员工 cust 表示客户
    )
select 'mp_'||s.id :: varchar share_id, -- 通过小程序分享
       s.share_no             emp_cd,
       s.share_time           share_dtm,
       s.share_channel        share_chnl,
       'zhibo_'||s.act_id::varchar   info_key,
       i.info_nm,
       'mp'                   src_sys_cd
    -- case when s.user_type='0' then 'fp' when s.user_type='1' then 'emp' else 'cust' end share_from_who
from ${schm}.cms_mp_act_share s
       left join edw.dim_finc_info i on 'zhibo_'||s.act_id::varchar = i.info_id
where s.share_time >= current_date - ${p_days}
  and status = '0'
  and s.parent_id is null
  and s.user_type = '0' ;-- 需要status=0

-- 主要记录分享记录，以便后面关联join