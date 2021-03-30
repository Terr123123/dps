truncate table stg.stg_dim_cust;
-- CRM 为主数据
insert into stg.stg_dim_cust(
        cust_no,
        cust_nm,
        cert_type,
        cert_cd,
        cust_show_nm,
        is_org,
        cust_sex,
        emp_cd,
        comp_id,
        mbr_stat,
        mbr_admit_dt,
        mbr_no,
        cust_bday,
        crm_cust_id,
        is_emp,
        crt_dtm,
        cust_from,
        cust_risk_lvl,
        src_sys_cd
)
select
        t.cust_no,
        max(t.cust_nm) cust_nm,
        min(t.cert_type) cert_type,
        max(t.cert_cd) cert_cd,
        max(t.cust_nm) cust_show_nm,
        max(t.is_org) is_org,
        max(t.cust_sex) cust_sex,
        max(t.emp_cd) emp_cd,
        min(coalesce(e.comp_id,709)) comp_id,
        min(t.mbr_stat) mbr_stat,
        min(t.admit_dt) mbr_admit_dt,
        max(t.mbr_no) mbr_no,
        max(t.cust_bday) cust_bday,
        max(t.src_sys_cust_id) crm_cust_id,
        max(t.is_emp) is_emp,
        min(t.crt_dtm) crt_dtm,
        max(t.cust_from) cust_from,
        max(t.cust_risk_lvl) cust_risk_lvl,
        max(t.src_sys_cd) src_sys_cd
from edw.dim_accnt t
left join edw.dim_emp e on t.emp_cd=e.emp_cd
where src_sys_cd='crm' and cust_no>'' group by  t.cust_no;

-- 私募数据为辅助
insert into stg.stg_dim_cust(
        cust_no,
        cust_nm,
        cert_type,
        cert_cd,
        cust_show_nm,
        is_org,
        cust_sex,
        emp_cd,
        comp_id,
        mbr_stat,
        mbr_admit_dt,
        mbr_no,
        cust_bday,
        crm_cust_id,
        is_emp,
        crt_dtm,
        cust_from,
        cust_risk_lvl,
        src_sys_cd
)
select
        t.cust_no,
        max(t.cust_nm) cust_nm,
        min(t.cert_type) cert_type,
        max(t.cert_cd) cert_cd,
        max(t.cust_nm) cust_show_nm,
        max(t.is_org) is_org,
        max(t.cust_sex) cust_sex,
        max(t.emp_cd) emp_cd,
        min(coalesce(e.comp_id,709)) comp_id,
        min(t.mbr_stat) mbr_stat,
        min(t.admit_dt) mbr_admit_dt,
        max(t.mbr_no) mbr_no,
        max(t.cust_bday) cust_bday,
        max(t.src_sys_cust_id) crm_cust_id,
        max(t.is_emp) is_emp,
        min(t.crt_dtm) crt_dtm,
        max(t.cust_from) cust_from,
        max(t.cust_risk_lvl) cust_risk_lvl,
        max(t.src_sys_cd) src_sys_cd
from edw.dim_accnt t
left join edw.dim_emp e on t.emp_cd=e.emp_cd
left join stg.stg_dim_cust c on t.cust_no=c.cust_no
where t.src_sys_cd='pf' and t.cust_no>'' and c.cust_no is null
group by  t.cust_no;

-- 海外基金
insert into stg.stg_dim_cust(
        cust_no,
        cust_nm,
        cert_type,
        cert_cd,
        cust_show_nm,
        is_org,
        cust_sex,
        emp_cd,
        comp_id,
        mbr_stat,
        mbr_admit_dt,
        mbr_no,
        cust_bday,
        crm_cust_id,
        is_emp,
        crt_dtm,
        cust_from,
        cust_risk_lvl,
        src_sys_cd
)
select
        t.cust_no,
        max(t.cust_nm) cust_nm,
        min(t.cert_type) cert_type,
        max(t.cert_cd) cert_cd,
        max(t.cust_nm) cust_show_nm,
        max(t.is_org) is_org,
        max(t.cust_sex) cust_sex,
        max(t.emp_cd) emp_cd,
        min(coalesce(e.comp_id,709)) comp_id,
        min(t.mbr_stat) mbr_stat,
        min(t.admit_dt) mbr_admit_dt,
        max(t.mbr_no) mbr_no,
        max(t.cust_bday) cust_bday,
        max(t.src_sys_cust_id) crm_cust_id,
        max(t.is_emp) is_emp,
        min(t.crt_dtm) crt_dtm,
        max(t.cust_from) cust_from,
        max(t.cust_risk_lvl) cust_risk_lvl,
        max(t.src_sys_cd) src_sys_cd
from edw.dim_accnt t
left join edw.dim_emp e on t.emp_cd=e.emp_cd
left join stg.stg_dim_cust c on t.cust_no=c.cust_no
where t.src_sys_cd='of' and t.cust_no>'' and c.cust_no is null
group by  t.cust_no;

-- 其他系统
insert into stg.stg_dim_cust(
        cust_no,
        cust_nm,
        cert_type,
        cert_cd,
        cust_show_nm,
        is_org,
        cust_sex,
        emp_cd,
        comp_id,
        mbr_stat,
        mbr_admit_dt,
        mbr_no,
        cust_bday,
        crm_cust_id,
        is_emp,
        crt_dtm,
        cust_from,
        cust_risk_lvl,
        src_sys_cd
)
select
        t.cust_no,
        max(t.cust_nm) cust_nm,
        min(t.cert_type) cert_type,
        max(t.cert_cd) cert_cd,
        max(t.cust_nm) cust_show_nm,
        max(t.is_org) is_org,
        max(t.cust_sex) cust_sex,
        max(t.emp_cd) emp_cd,
        max(coalesce(e.comp_id,709)) comp_id,
        null mbr_stat, -- 根据交易计算 有交易的和没交易的
        null mbr_admit_dt,
        null mbr_no,
        max(t.cust_bday) cust_bday,
        null crm_cust_id,
        max(t.is_emp) is_emp,
        min(t.crt_dtm) crt_dtm,
        max(t.cust_from) cust_from,
        max(t.cust_risk_lvl) cust_risk_lvl,
        'other' src_sys_cd
from edw.dim_accnt t
left join edw.dim_emp e on t.emp_cd=e.emp_cd
left join stg.stg_dim_cust c on t.cust_no=c.cust_no
where  t.cust_no>'' and c.cust_no is null
group by  t.cust_no;

-- 对没有emp_cd 的客户数据修正
with tmp_cust as(
    select
    t.cust_no,
    max(t.emp_cd) emp_cd,
    coalesce(max(e.comp_id),709) comp_id
    from
    edw.dim_accnt t
    left join edw.dim_emp e on t.emp_cd=e.emp_cd
    inner join stg.stg_dim_cust c on t.cust_no=c.cust_no and (c.emp_cd is null or c.emp_cd ='')
    where t.src_sys_cd not in('pf','crm') and t.emp_cd>''
    group by t.cust_no)
update stg.stg_dim_cust cust
     set emp_cd=tmp_cust.emp_cd,comp_id=tmp_cust.comp_id
from tmp_cust where cust.cust_no=tmp_cust.cust_no
                and (cust.emp_cd='' or cust.emp_cd is null)