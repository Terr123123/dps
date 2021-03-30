delete from edw.fact_cust_pay where paid_crt_dtm>=current_date-${p_days} or paid_crt_dtm is null;
insert into edw.fact_cust_pay(
    paid_no   ,
    src_sys_cd  ,
    req_seq ,
    mgr_capi_seq_id ,
    emp_cd  ,
    comp_id ,
    paid_dt ,
    paid_amnt   ,
    pay_arrv_dt ,
    src_order_no    ,
    pay_arrv_amnt   ,
    accnt_cd    ,
    cust_no ,
    is_valid    ,
    mgr_acct_cd ,
    is_arrv,
    paid_crt_dtm
    )
with tp_c as(
    select s_cust_id cust_id,min(cust_no) cust_no
        from  cust.trans_acnt_info
        where s_code='PRIFUND' group by s_cust_id
    )
SELECT  'pf_'||b.id paid_no,
        'pf' as src_sys_cd,
        b.req_seq req_seq,
        b.mgr_capi_seq_id ,
        b.mgr_code emp_cd,
        b.brah_id comp_id,
        b.trad_date paid_dt,
        b.amount paid_amnt,
        c.trad_date pay_arrv_dt,
        b.order_no src_order_no,
        c.amt pay_arrv_amnt,
         'pf_'||b.cust_id accnt_cd,
        tp_c.cust_no,
        case when b.rec_stat='1' then 'Y' else 'N' end is_valid,
        c.mgr_acct_no mgr_acct_cd,
        case when c.trad_date>'2000-01-01' then 'Y' else 'N' end is_arrv,
        b.create_time paid_crt_dtm
FROM  prifund.cust_capi_seq b
LEFT JOIN prifund.cust_tran_capi_rec c ON b.mgr_capi_seq_id=c.id and c.rec_stat='1'
left join tp_c  on   tp_c.cust_id=b.cust_id::varchar
where b.create_time>=current_date-${p_days};
