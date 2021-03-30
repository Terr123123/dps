TRUNCATE TABLE edw.dim_prod_open;
INSERT INTO edw.dim_prod_open (prod_open_id,
                               prod_id,
                               open_rank_nm,
                               open_beg_dt,
                               open_end_dt,
                               ordr_beg_dt,
                               ordr_end_dt,
                               open_type)
SELECT 'pf_'|| max(o.id)                                                        prod_open_id,
       'pf_'|| prod_id :: varchar                                          prod_id,
       row_number() over (PARTITION BY o.prod_id ORDER BY o.open_beg_date) open_rank_nm,
       max(open_beg_date)                                                       open_beg_dt,
       max(open_end_date)                                                       open_end_dt,
       max(order_beg_date)                                                     ordr_beg_dt,
       max(order_end_date)                                                      ordr_end_dt,
       '开放期'                                                               open_type
    --     CASE WHEN sg_stat='1' THEN 'Y' ELSE 'N' END sg_stat,
    --     CASE WHEN sh_stat='1' THEN 'Y' ELSE 'N' END sh_stat,
    --     CASE WHEN zr_stat='1' THEN 'Y' ELSE 'N' END zr_stat
FROM ods.pf_prod_open_days o
WHERE o.rec_stat = '1' group by prod_id,open_beg_date;
INSERT INTO edw.dim_prod_open (prod_open_id,
                               prod_id,
                               open_rank_nm,
                               open_beg_dt,
                               open_end_dt,
                               ordr_beg_dt,
                               ordr_end_dt,
                               open_type)
SELECT 'pf_'||a.id||'_0'  prod_open_id,
       'pf_'||a.id     AS prod_id,
       0                  open_rank_nm,
       a.rais_beg_date AS beg_date,
       a.rais_end_date AS end_date,
       a.rais_beg_date AS beg_date,
       a.rais_end_date AS end_date,
       '募集期'              open_type
FROM ods.pf_prod_info a
left join  ods.pf_prod_open_days o on a.id=o.prod_id and a.rais_beg_date=o.open_beg_date
where a.rais_beg_date >'2000-01-01' and o.id is null;