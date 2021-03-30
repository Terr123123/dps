delete from edw.dim_busi_dev_user where crt_dtm >= current_date - ${p_days};
-- 小程序用户
insert into edw.dim_busi_dev_user (open_id,
                                   union_id,
                                   wx_nick_nm,
                                   wx_photo_url,
                                   crm_cust_no,
                                   emp_no,
                                   user_type,
                                   src_accnt_id,
                                   gender,
                                   city_nm,
                                   prov_nm,
                                   crty_nm,
                                   crt_dtm,
                                   etl_dtm)
select a.open_id                                                                    open_id,
       a.open_id                                                                    union_id,
       a.nick_name                                                                  wx_nick_nm,
       a.avatar_url                                                                 wx_photo_url,
       a.cust_no,
       a.emp_no,
       'mp'                                                                         user_type,  -- 小程序用户 单纯为了和微信用户区分，其实也是微信用户
       a.id :: varchar                                                              mp_accnt_id,
       case when a.gender = '1' then '男' when a.gender = '2' then '女' else '未知' end gender,
       a.city                                                                       city_nm,
       a.province                                                                   prov_nm,
       a.country                                                                    crty_nm,
       a.create_time                                                                crt_dtm,
       current_timestamp                                                            etl_dtm
from ${schm}.cms_mp_account a
       left join edw.dim_busi_dev_user d on a.open_id = d.open_id
where d.open_id is null
  and a.create_time >= current_date - ${p_days};
-- 牛头帮数据
insert into edw.dim_busi_dev_user (open_id,
                                   union_id,
                                   wx_nick_nm,
                                   wx_photo_url,
                                   crm_cust_no,
                                   emp_no,
                                   user_type,
                                   src_accnt_id,
                                   gender,
                                   city_nm,
                                   prov_nm,
                                   crty_nm,
                                   crt_dtm,
                                   etl_dtm)
select i.openid,
       i.unionid,
       i.nickname,
       i.avatar                                                           wx_photo_url,
       null                                                               crm_cust_no,
       null                                                               emp_cd,
       'wechat_info'                                                      user_type, -- 牛头帮开发的其实也有小程序用户
       i.id                                                               src_accnt_id,
       case when i.sex = 1 then '男' when i.sex = 2 then '女' else '未知' end gender,
       i.city                                                             city_nm,
       i.province                                                         prov_nm,
       i.country                                                          crty_nm,
       i.time_created                                                     crt_dtm,
       current_timestamp                                                  etl_dtm
from ${schm}.busi_dev_wechat_info i
       left join edw.dim_busi_dev_user d on i.openid = d.open_id
where d.open_id is null
  and i.time_created >= current_date - ${p_days};
-- 牛头帮名片数据
insert into edw.dim_busi_dev_user(open_id,
                                   union_id,
                                   wx_nick_nm,
                                   wx_photo_url,
                                   crm_cust_no,
                                   emp_no,
                                   user_type,
                                   src_accnt_id,
                                   crt_dtm,
                                   etl_dtm)
select i.user_id         openid,
       i.user_id         unionid,
       i.nickname        wx_nick_nm,
       i.avatar          wx_photo_url,
       null              crm_cust_no,
       i.user_id         emp_cd,
       'card'             user_type,
       i.id              src_accnt_id,
       i.time_created    crt_dtm,
       current_timestamp etl_dtm
from ${schm}.busi_dev_card i
       left join edw.dim_busi_dev_user d on i.user_id = d.open_id
where d.open_id is null
  and i.time_created >= current_date - ${p_days};
