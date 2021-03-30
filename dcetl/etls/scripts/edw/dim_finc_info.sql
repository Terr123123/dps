delete from edw.dim_finc_info where crt_dtm >= current_date - ${p_days};
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
                               info_url, --  资讯链接
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
                               info_author, --  资讯作者
                               info_sum_cntnt, -- 资讯摘要
                               src_sys_info_id,-- 源系统资讯id
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
                               )
select
    id info_cd,
    title info_nm,
    url info_url,
    lower(channel) info_from,
    '网络文章' info_type,
    avatar info_pic_url,
    author info_author,
    summary info_sum_cntnt,
    id src_sys_info_id,
    time_created crt_dtm,
    time_published pub_dtm
from ods.busi_dev_article where time_created >= current_date - ${p_days};
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
                               info_url, --  资讯链接
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
                               info_mini_pic_url, --  迷你图片链接
                               info_audio_url, --  多媒体链接
                               src_sys_info_id,
                               vrst_cnt, --  访问次数
                               info_author, --  资讯作者
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
    )
select 'yjy_'||id::varchar          info_id, -- 海银研究院
       title                  info_nm,
       null                   info_url,
       '海银研究院'             info_from,
       '每日视角&研报' :: varchar info_type,
       image                  info_pic_url,
       thumbnail              info_mini_pic_url,
       audio_file             info_audio_url,
       id src_sys_info_id,
       visit_num              vrst_cnt,
       author                 info_author,
       create_time            crt_dtm,
       publish_date           pub_dtm
from ${schm}.cms_article
where is_delete = '0'
  and create_time >= current_date - ${p_days};
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
                               info_url, --  资讯链接
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
                               info_mini_pic_url, --  迷你图片链接
                               info_audio_url, --  多媒体链接
                               src_sys_info_id,
                               vrst_cnt, --  访问次数
                               info_author, --  资讯作者
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
    )
select 'hycb_'||id::varchar     info_id,
       title             info_nm,
       null              info_url,
       '海银晨报'         info_from,
       '海银晨报' :: varchar info_type,
       image             info_pic_url,
       thumbnail         info_mini_pic_url,
       audio_file        info_audio_url,
      id src_sys_info_id,
       visit_num         vrst_cnt,
       '海银财富'              info_author,
       create_time       crt_dtm,
       publish_date      pub_dtm
from ${schm}.cms_daily_news
where is_delete = '0'
  and create_time >= current_date - ${p_days};
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
                               info_url, --  资讯链接
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
                               info_mini_pic_url, --  迷你图片链接
                               info_audio_url, --  多媒体链接
                               vrst_cnt, --  访问次数
                               info_author, --  资讯作者
                               src_sys_info_id,
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
    )
select 'zhibo_'||id::varchar  info_id,
       title_zh              info_nm,
       null                  info_url,
       '空中课堂'                info_from,
       '空中课堂&V直播' :: varchar info_type,
       share_image           info_pic_url,
       null                  info_mini_pic_url,
       video_link            info_audio_url,
       visit_num             vrst_cnt,
       '空中课堂'                info_author,
       id::varchar       src_sys_info_id,
       create_time           crt_dtm,
       publish_date          pub_dt
from ${schm}.cms_mp_activity
where create_time >= current_date - ${p_days};
-- 微名片处理
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
                               info_mini_pic_url, --  迷你图片链接
                               src_sys_info_id,-- 源系统资讯id
                               info_author,
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
    )
select user_id    info_cd,
       name          info_nm,
       '微名片'            info_from,
       'card' :: varchar info_type,
       avatar      info_pic_url,
       qr_code     info_mini_pic_url,
       id src_sys_info_id,-- 源系统资讯id
       name info_author,
       time_created      crt_dtm,
       time_created      pub_dtm
from ${schm}.busi_dev_card
where time_created >= current_date - ${p_days};
-- 牛头帮headline,NEWs表示晨报，headlline外部头条资讯
insert into edw.dim_finc_info (info_id, --  资讯ID
                               info_nm, --  资讯名称
--                                info_url, --  资讯链接
                               info_from, --  资讯来源
                               info_type, --  资讯类型
                               info_pic_url, --  图片链接
--                                info_author, --  资讯作者
                               info_sum_cntnt, -- 资讯摘要
                               src_sys_info_id,-- 源系统资讯id
                               crt_dtm, --  创建时间
                               pub_dtm      --  发布时间
                               )
select
    id info_cd,
    title info_nm,
--    url info_url,
    '资讯头条' info_from,
    lower(headline_news) info_type, --牛头帮headline,NEWs表示晨报，headlline外部头条资讯
    avatar info_pic_url,
--    author info_author,
    title info_sum_cntnt,
    id src_sys_info_id,
    time_created crt_dtm,
    time_published pub_dtm
from ods.busi_dev_headline where time_created >= current_date - ${p_days};
