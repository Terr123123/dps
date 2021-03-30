truncate  table edw.dim_accnt;
insert into edw.dim_accnt(
    accnt_cd,
    src_sys_cust_id	,
    src_sys_cust_cd	,
    src_sys_cd	,
    cust_nm	,
    is_org	,
    emp_cd	,
    mbr_no	,
    admit_dt	,
    cert_type	,
    cert_cd	,
    is_secret_to_fmly	,
    cust_from	,
    curr_cust_grade	,
    cust_risk_lvl	,
    cust_sex	,
    crt_dtm	,
    cust_curr_stat	,
    is_emp,
    cust_bday,
		cust_no
)

select
    t.accnt_cd,
    t.src_sys_cust_id	,
    t.src_sys_cust_cd	,
    t.src_sys_cd	,
    t.cust_nm	,
    t.is_org	,
    case when t.emp_cd>'' then t.emp_cd else coalesce(p.emp_cd,null) end emp_cd	,
    t.mbr_no	,
    t.admit_dt	,
    t.cert_type	,
    t.cert_cd	,
    t.is_secret_to_fmly	,
    t.cust_from	,
    t.curr_cust_grade	,
    t.cust_risk_lvl	,
    t.cust_sex	,
    t.crt_dtm	,
    t.cust_curr_stat	,
    case when p.emp_cd>'' then 'Y' else 'N' end is_emp,
    cust_bday,
		p.cust_no
from edw.stg_dim_accnt t
left join cust.cust_no_pool p on p.cert_code=t.cert_cd and p.cert_type=t.cert_type
;