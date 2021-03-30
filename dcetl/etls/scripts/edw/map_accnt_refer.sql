truncate table edw.map_accnt_refer;
insert into edw.map_accnt_refer(
     src_sys_cd,
     rltn_nm,
     accnt_cd,
     refer_accnt_cd, -- 推荐人账号
     crt_dtm
    )
with dt as (
     select case when group_cd='RELATION_BIZ' then '02' else '01' end cust_type, -- 机构和个人分开
            dic_key,
            dic_nm
     from dd.src_sys_dic_map
     where group_cd in('RELATION_BIZ','RELATION_PER')
       and sys_alia_cd='crm'
     )
select
     'crm' src_sys_cd,
     coalesce(dt.dic_nm,'其他') rltn_nm,
     'crm_'||r.cust_id::varchar  accnt_cd,
     'crm_'||ref_cust refer_accnt_cd, -- 推荐人账号
     create_time crt_dtm
from
     crm.hy_cust_refer r left join dt
     on r.cust_class=dt.cust_type and r.relation=dt.dic_key
     where r.is_effect='01';