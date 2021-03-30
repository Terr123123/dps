delete from edw.fact_pre_order where order_dtm>=current_date-${p_days};
insert into  edw.fact_pre_order(
        order_no    ,
        src_order_no    ,
        emp_cd  ,
        order_dtm   ,
        comp_id ,
        accnt_cd    ,
        cust_no ,
        prod_id ,
        shr_type_id ,
        order_amnt   ,
        chk_stat_id ,
        chh_stat_nm ,
        used_stat_id    ,
        used_stat_nm    ,
        conf_stat_id    ,
        conf_stat_nm    ,
        order_cont_no   ,
        is_valid_order
)
with tp_c as(
    select s_cust_id cust_id,min(cust_no) cust_no
        from  cust.trans_acnt_info
        where s_code='PRIFUND' group by s_cust_id
    )
        SELECT
              -- cust.trans_acnt_info账户信息有重复
              'pf_'||a.id::varchar order_no,
               a.order_no as src_order_no,
               a.mgr_code emp_cd,
               a.order_time order_dtm,
               a.brah_id comp_id,
              'pf_'||a.cust_id::varchar accnt_cd,
               c.cust_no,
              'pf_'||a.prod_id prod_id,
              'pf_'||a.shr_type_id shr_type_id,
               a.resv_amt ordr_amnt,
               a.chk_stat chk_stat_id,
               m1.dic_nm chh_stat_nm,
               a.used_stat used_stat_id,
               m2.dic_nm used_stat_nm,
               a.conf_stat conf_stat_id,
               m3.dic_nm conf_stat_nm,
               a.order_cont_no,
               case when
               a.CHK_STAT IN ('0', '1')        -- 审核状态
                AND a.USED_STAT IN ('0','1','2','6') -- '0'-未成立 '1'-已成立 '2'-已使用 '6'-资金对账成功
                AND a.conf_stat IN ('1') then 'Y' else 'N' end is_valid_order       -- 有效预约
        FROM prifund.order_amt_req a
        left join tp_c c on   c.cust_id=a.cust_id::varchar
        left join dd.src_sys_dic_map m1 on m1.sys_alia_cd='pf' and m1.group_cd = 'ORDER_CHK_STAT' and m1.dic_key=a.chk_stat
        left join dd.src_sys_dic_map m2 on m2.sys_alia_cd='pf' and m2.group_cd = 'ORDER_USE_STAT' and m2.dic_key=a.used_stat
        left join dd.src_sys_dic_map m3 on m3.sys_alia_cd='pf' and m3.group_cd = 'RESERVE_CONF_STAT' and m3.dic_key=a.conf_stat
        WHERE a.rec_stat='1' and a.order_time>=current_date-${p_days}