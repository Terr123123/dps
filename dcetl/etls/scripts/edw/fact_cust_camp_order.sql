delete from edw.fact_cust_camp_order where camp_start_dt>= current_date - ${p_days};
insert into edw.fact_cust_camp_order (camp_cd,
                                      src_camp_id,
                                      accnt_cd,
                                      emp_cd,
                                      is_new_cust,
                                      camp_start_dt,
                                      src_sys_cd,
                                      esti_sign_amnt,
                                      arrv_flag)
select 'crm_'||camp_id                                    camp_cd,
       camp_id                                            src_camp_id,
       'crm_'||cust_id                                    accnt_cd,
       c.owner_code                                       emp_cd,
       case when i.cust_flag = '01' then 'Y' else 'N' end is_new_cust,
       f.start_time :: date                               camp_start_dt,
       'crm'                                              src_sys_cd,
       i.prepare_signamt                                  esti_sign_amnt,
    -- f.start_time::date+90  trans_end_dt,
    -- (lag(f.start_time::date) over (partition by cust_id order by f.start_time desc))-1 end_dt,
       case when arrive_stat = '01' then 1 else 0 end     arrv_flag -- 1已到  0未到
from ${schm}.crm_hy_camp_order i
       inner join ${schm}.crm_hy_camp_info f on i.camp_id = f.id and f.start_time >= current_date - ${p_days}
       left join ${schm}.crm_hy_cust_info c on c.id = i.cust_id
where i.effective = '01';