truncate  table stg.stg_dim_accnt;
-- CRM 用户数据
insert into stg.stg_dim_accnt(
    accnt_cd,
    src_sys_cust_id	,
    src_sys_cust_cd	,
    src_sys_cd	,
    cust_nm	,
    is_org	,
    emp_cd	,
    mbr_no	,
    mbr_stat ,
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
--     is_emp,
    cust_bday
)
        select
            'crm_'||c.id::varchar  accnt_cd,
            c.id src_sys_cust_id,
            c.old_custid src_sys_cust_cd,
            'crm' as src_sys_cd,
            c.cust_name cust_nm,
            case when c.cust_class='02' then 'Y' else 'N' end is_org,
            case when trim(c.owner_code)='' then null else trim(c.owner_code) end emp_cd,
            c.member_no mbr_no, -- 会员编号
            CASE TRIM(c.cust_type)
              WHEN '01' THEN '01'
              WHEN '02' THEN '03'
              WHEN '03' THEN '02'
              ELSE TRIM(cust_type) END mbr_stat,  -- 修改后 01 正式 02 会员 03 潜在
            c.admit_time::date admit_dt, -- 入会日期
            CASE
                 WHEN TRIM(c.cert_type)='08' THEN '07'
                 WHEN TRIM(c.cert_type)='11' THEN '11'
                 -- CRM三证合一对应其他系统营业执照
                 WHEN TRIM(c.cert_type)='13' THEN '11'
                 WHEN TRIM(c.cert_type)='14' THEN '18'
                 ELSE TRIM(c.cert_type) end cert_type,
            case when TRIM(c.cert_type)='00'then id15to18(c.cert_code) else c.cert_code end cert_cd,
            case when c.secret='01' then 'N' when c.secret='02' then 'Y' else 'Y' end is_secret_to_fmly,
            coalesce(m2.dict_nm,'未知') cust_from, -- c.is_badrecord,c.source_desc,c1.cust_name,
            coalesce(m.dict_nm,'会员') curr_cust_grade,
             m3.dict_nm cust_risk_lvl,
             case when c.cust_class='02' then '机构'
                when c.link_sex='01' then '男' when c.link_sex='02' then '女' else '未知' end cust_sex,
              c.create_time crt_dtm,-- c.create_by,
             m4.dict_nm curr_stat,
--              case when e.cert_cd>'' then 'Y' else 'N' end is_emp,
             p.birthday::date cust_bday
        from ods.crm_hy_cust_info c
        left join edw.dict_src_sys m on m.group_cd='CUST_GRADE' and m.sys_alia_cd='crm' and m.dict_key=c.cur_grade
        left join edw.dict_src_sys m1 on m1.group_cd='CERT_TYPE' and m1.sys_alia_cd='crm' and m1.dict_key=c.cert_type
        left join edw.dict_src_sys m2 on m2.group_cd='CUST_SOURCE' and m2.sys_alia_cd='crm' and m2.dict_key=c.cust_source
--         left join tp_emp e on e.cert_cd=c.cert_code
        left join ods.crm_hy_cust_per p on c.id=p.cust_id
        left join edw.dict_src_sys m3 on m3.group_cd='RISK_LEVEL_NEW' and m3.sys_alia_cd='crm' and m3.dict_key=c.risk_level
        left join edw.dict_src_sys m4 on m4.group_cd='EXISTSTATUS' and m4.sys_alia_cd='crm' and m4.dict_key=c.exist_status;
        -- left join edw.dict_src_sys m5 on m5.group_cd='CUST_TYPE' and m5.sys_alia_cd='crm' and m5.dict_key=c.cust_type;

-- 海银基金用户数据
insert into stg.stg_dim_accnt(
    accnt_cd,
    src_sys_cust_id	,
    src_sys_cust_cd	,
    src_sys_cd	,
    cust_nm	,
    is_org	,
    emp_cd	,
    mbr_no	,
    mbr_stat,
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
--     is_emp,
    cust_bday
)
--   with e as (select distinct cert_cd from edw.dim_emp  )
     SELECT
    'pof_'||a.custid::varchar  accnt_cd,
    a.custid AS src_sys_cust_id,
    a.custid AS src_sys_cust_cd,
    'pof' as src_sys_cd,
     a.investorname AS cust_nm,
    case when a.individualorinstitution<>'1' then 'Y' else 'N' end is_org,
    null emp_cd,
    null mbr_no, -- 会员编号
    null mbr_stat,
    null admit_dt, -- 入会日期
    b.dps_cert_type AS cert_type,
     case when b.dps_cert_type ='00'then id15to18(a.certificateno) else a.certificateno end cert_cd,
    null is_secret_to_fmly,
    null cust_from,
    null curr_cust_grade,
    'C'||m3.dict_key||m3.dict_nm  cust_risk_lvl,
    CASE WHEN a.individualorinstitution='1' THEN
        case when c.sex='1' then '男' when c.sex='0' then '女' else '未知' end
         else '机构' END AS cust_sex,
    to_timestamp(a.registdate, 'YYYYMMDD') AS crt_dtm,
    CASE a.custstatus WHEN '0' THEN '销户' WHEN '1' THEN '正常' WHEN '2' THEN '冻结' END AS curr_stat,
--     case when e.cert_cd>'' then 'Y' else 'N' end is_emp,
     CASE WHEN a.individualorinstitution='1' THEN to_date(c.birthday, 'YYYYMMDD') else null END AS cust_bday
    FROM ods.hyf_order_cust_custinfo a
    LEFT JOIN dd.cert_type_compare b ON (CASE WHEN a.individualorinstitution='1' THEN '1' ELSE '0' END)=b.cust_type
        AND a.certificatetype=b.cert_type AND b.s_code='FUND'
--     left join e on e.cert_cd=a.certificateno
    LEFT JOIN ods.hyf_order_cust_custinfo_append_p c ON a.custid=c.custid
   left join edw.dict_src_sys m3 on m3.group_cd='CUSTOMER_RISK_LEVEL' and m3.sys_alia_cd='hyf' and m3.dict_key=a.custrisklevel;

-- 私募基金客户数据
insert into stg.stg_dim_accnt(
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
--     is_emp,
    cust_bday
)
    select
    'pf_'||b.id::varchar  accnt_cd,
     b.id AS src_sys_cust_id,
     b.cust_no AS src_sys_cust_cd,
    'pf' as src_sys_cd,
    b.cust_name cust_nm,
    case when b.cust_type<>'1' then 'Y' else 'N' end is_org,
    case when trim(b.mgr_code)='' then null else trim(b.mgr_code) end emp_cd,
    null mbr_no, -- 会员编号
    null admit_dt, -- 入会日期
     t.dps_cert_type cert_type,
     case when t.dps_cert_type ='00'then id15to18(TRIM(b.cert_no)) else TRIM(b.cert_no) end cert_cd,
    null is_secret_to_fmly,
    null cust_from,
    null curr_cust_grade,
    'C'||m3.dict_key||m3.dict_nm cust_risk_lvl,
    CASE WHEN TRIM(b.cust_type)='1' THEN
        case when b.sex='1' then '男' when b.sex='2' then '女' else '未知' end
         else '机构' END AS cust_sex,
    b.create_time AS crt_dtm,
    null curr_stat,
--      case when e.cert_cd>'' then 'Y' else 'N' end is_emp,
    case when t.dps_cert_type='00' then str_to_date(substr(trim(b.cert_no),7,8)) else null end cust_bday
    FROM
    ods.pf_cust_info b
    left join dd.cert_type_compare t on t.s_code='PRIFUND' and t.cert_type=b.cert_type
--     left join e on trim(b.cert_no)=e.cert_cd
    left join edw.dict_src_sys m3 on m3.group_cd='CUST_RISK_LEVEL' and m3.sys_alia_cd='pf' and m3.dict_key=b.cust_risk_level
    where b.rec_stat='1'
    ;
-- 海外基金客户信息
insert into stg.stg_dim_accnt(
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
--     is_emp,
    cust_bday
)
    select
       'of_'||b.id::varchar  accnt_cd,
       b.id AS src_sys_cust_id,
       b.cust_no AS src_sys_cust_cd,
       'of' as src_sys_cd,
       b.cust_name cust_nm,
       case when b.cust_type<>'1' then 'Y' else 'N' end is_org,
       case when trim(b.mgr_code)='' then null else trim(b.mgr_code) end emp_cd,
       null mbr_no, -- 会员编号
       null admit_dt, -- 入会日期
       t.dps_cert_type cert_type,
       case when t.dps_cert_type ='00'then id15to18(TRIM(b.cert_no)) else TRIM(b.cert_no) end cert_cd,
       null is_secret_to_fmly,
       null cust_from,
       null curr_cust_grade,
      'C'||m3.dict_key||m3.dict_nm cust_risk_lvl,
       CASE WHEN TRIM(b.cust_type)='1' THEN
          case when b.sex='1' then '男' when b.sex='2' then '女' else '未知' end
           else '机构' END AS cust_sex,
       b.create_time AS crt_dtm,
       null curr_stat,
--        case when e.cert_cd>'' then 'Y' else 'N' end is_emp,
       case when t.dps_cert_type='00' then str_to_date(substr(trim(b.cert_no),7,8)) else null end cust_bday
    FROM
    ods.of_cust_info b
    left join dd.cert_type_compare t on t.s_code='AFUND' and t.cert_type=b.cert_type
--     left join e on trim(b.cert_no)=e.cert_cd
    left join edw.dict_src_sys m3 on m3.group_cd='CUST_RISK_LEVEL'
                                         and m3.sys_alia_cd='pf'
                                         and m3.dict_key=b.cust_risk_level -- 自己没有借用私募基金字典
where  b.rec_stat='1';

-- 海银会客户信息 海银会已经关闭 可以把海银会的客户数据锁定单独存放一张表
insert into stg.stg_dim_accnt(
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
--     is_emp,
    cust_bday
)
    SELECT
    'hyh_'||cust_id AS accnt_cd,
    cust_id AS src_sys_cust_id,
    cust_id AS src_sys_cust_cd,
    'hyh' as src_sys_cd,
    cust_name AS cust_nm,
    case when cust_type<>'1' then 'Y' else 'N' end is_org,
    null emp_cd,
    null mbr_no, -- 会员编号
    null admit_dt, -- 入会日期
    cert_type,
    case when cert_type ='00'then id15to18(TRIM(cert_code)) else TRIM(cert_code) end cert_cd,
--     CASE cust_type WHEN '1' THEN '1' WHEN '2' THEN '0' ELSE cust_type END AS cust_type,
    null is_secret_to_fmly,
    null cust_from,
    null curr_cust_grade,
    null cust_risk_lvl,
    CASE WHEN TRIM(cust_type)='1' THEN
              case when cert_type='00' and  SUBSTRING(id15to18(TRIM(cert_code)),17,1)::int%2=1  then '男'
                   when cert_type='00' and  SUBSTRING(id15to18(TRIM(cert_code)),17,1)::int%2=0 then '女' else '未知' end
         else '机构' END AS cust_sex,
    create_time::timestamp crt_dtm,
    null curr_stat,
--     case when e.cert_cd>'' then 'Y' else 'N' end is_emp,
    case when cert_type='00' then str_to_date(substr(id15to18(TRIM(cert_code)),7,8)) else null end cust_bday
    FROM hyh.cust_INVAPI004 b
    -- left join e on trim(b.cert_code)=e.cert_cd;