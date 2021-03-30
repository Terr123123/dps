delete from  edw.fact_order_pf_log where crt_dtm>=current_date-${p_days};
insert into edw.fact_order_pf_log(trans_cd,oper_type_cd,oper_type_nm,is_conf_oper,crt_dtm)
select
    l.req_id trans_cd,
--    l.trad_code,
    l.oper_type oper_type_cd,
    m.dict_nm oper_type_nm,
    case when oper_type IN ('07', '37') then 'Y' else 'N' end is_conf_oper,
    l.create_time crt_dtm
from ods.pf_trad_req_log l
left join edw.dict_src_sys m
on l.oper_type=m.dict_key and m.group_cd ='TRADE_OPER_TYPE' and m.sys_alia_cd='pf'
where   chk_stat = '1'  and rec_stat = '1' and  l.create_time>=current_date-${p_days};