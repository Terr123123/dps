truncate table edw.dim_cust;
-- stg_dim_cust 不涉及计算和复杂依赖。dim_cust可以加入一些复杂计算 例如当前状态
insert into edw.dim_cust(
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
        refer_cust_no,
        crm_cust_id,
        is_emp,
        crt_dtm,
        cust_from,
        cust_risk_lvl,
        src_sys_cd
)
with ref as(   -- 推荐客户
    select a1.cust_no,
          max(a2.cust_no) refer_cust_no
    from edw.map_accnt_refer r
        left join edw.dim_accnt a1 on r.accnt_cd=a1.accnt_cd
        left join edw.dim_accnt a2 on r.refer_accnt_cd=a2.accnt_cd
        group by a1.cust_no )
select
        t.cust_no,
        t.cust_nm,
        t.cert_type,
        t.cert_cd,
        t.cust_show_nm,
        t.is_org,
        t.cust_sex,
        t.emp_cd,
        t.comp_id,
        t.mbr_stat,  -- 注意 需要根据交易计算出结果
        t.mbr_admit_dt,
        t.mbr_no,
        t.cust_bday,
        ref.refer_cust_no::varchar refer_cust_no,
        t.crm_cust_id,
        t.is_emp,
        t.crt_dtm,
        t.cust_from,
        t.cust_risk_lvl,
        t.src_sys_cd
from stg.stg_dim_cust t
left join ref on t.cust_no=ref.cust_no;