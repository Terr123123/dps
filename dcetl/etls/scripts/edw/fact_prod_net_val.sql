delete
from edw.fact_prod_net_val
where net_val_dt >= current_date - ${p_days};
INSERT INTO edw.fact_prod_net_val (shr_type_id,
                                   prod_id,
                                   net_val_dt,
                                   net_val,
                                   accum_net_val,
                                   seven_day_aror,
                                   ten_thsd_acrl,
                                   crt_dtm,
                                   sys_sys_cd)
SELECT 'pf_'||n.shr_type_id :: varchar shr_type_id,
       'pf_'|| prod_id :: varchar      prod_id,
       n.netv_date                     net_val_dt,
       n.net_val,
       tot_net_val                     accum_net_val,
       seven_income                    seven_day_aror,  -- 7日年化收益率
       fund_income                     ten_thsd_acrl,   -- 万份收益
       n.create_time                   crt_dtm,
       'pf'                            sys_sys_cd
FROM ods.pf_prod_net_val n
WHERE n.REC_STAT = '1'
  and n.netv_date >= current_date - ${p_days};
INSERT INTO edw.fact_prod_net_val (prod_id, shr_type_id, net_val_dt, net_val, accum_net_val, --     seven_day_aror,
                                   ten_thsd_acrl, crt_dtm, sys_sys_cd)
select i.prod_id,
       '000000'                                                      shr_type_id,
       n.navdate :: date                                             net_val_dt,
       max(n.nav)                                                    net_val,
       max(n.accumulativenav)                                        accum_net_val,
       max(n.fundincome)                                             ten_thsd_acrl,
       to_timestamp(left(max(n.modifytime), 20), 'yyyyMMddhh24miss') crt_dtm,
       'hyf'                                                         sys_sys_cd
from ods.hyf_batch_trade_fundnav n
       inner join edw.dim_prod i on n.fundcode = i.prod_cd and i.prod_type = '公募产品'
where n.navdate >= to_char(current_date - ${p_days}, 'yyyyMMdd')
group by i.prod_id, n.navdate;
INSERT INTO edw.fact_prod_net_val (shr_type_id,
                                   prod_id,
                                   net_val_dt,
                                   net_val,
                                   accum_net_val,
                                   seven_day_aror,
                                   ten_thsd_acrl,
                                   crt_dtm,
                                   sys_sys_cd)
SELECT '000000' :: varchar        shr_type_id,
       'of_'|| prod_id :: varchar prod_id,
       n.netv_date                net_val_dt,
       n.net_val,
       tot_net_val                accum_net_val,
       seven_income               seven_day_aror,  -- 7日年化收益率
       fund_income                ten_thsd_acrl,   -- 万份收益
       n.create_time              crt_dtm,
       'of'                       sys_sys_cd
FROM ods.of_prod_net_val n
WHERE n.REC_STAT = '1'
  and n.netv_date >= current_date - ${p_days};