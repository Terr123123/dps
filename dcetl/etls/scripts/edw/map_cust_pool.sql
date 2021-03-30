-- insert into edw.map_cust_pool(
--       cert_type,
--       cert_cd
--     )
-- select
--       distinct
--        t.cert_type,
--        t.cert_cd
-- from edw.stg_dim_accnt t
-- left join edw.map_cust_pool p on p.cert_cd=t.cert_cd and p.cert_type=t.cert_type
-- where t.cert_cd>'' and t.cert_type>'' and p.cert_cd is null;

INSERT INTO cust.cust_no_pool(
     id,
     cust_no,
     cust_type,
     cert_type,
     cert_code
    )
with emp as (select distinct cert_cd from edw.dim_emp  ),
    temp_cust_cert as(
    select
          distinct
          case when t.is_org='Y' then '0' else '1' end cust_type,
           t.cert_type,
           t.cert_cd,
           emp.emp_cd
    from edw.stg_dim_accnt t
    left join cust.cust_no_pool p on p.cert_code=t.cert_cd and p.cert_type=t.cert_type
    left join emp on t.cert_cd=emp.cert_cd
    where t.cert_cd>'' and t.cert_type>'' and p.cert_code is null
)
SELECT
    nextval('cust.seq_cust_id') AS id,
    CASE a.cust_type WHEN '0' THEN '10'||nextval('cust.seq_cust_no')
    ELSE '11'||nextval('cust.seq_cust_no') END AS cust_no,
    a.cust_type,
    a.cert_type,
    a.cert_cd
    FROM temp_cust_cert a;