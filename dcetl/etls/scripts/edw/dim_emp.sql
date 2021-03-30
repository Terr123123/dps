truncate TABLE edw.dim_emp;
INSERT INTO edw.dim_emp(
        emp_cd,             -- 员工工号
        emp_nm,             -- 姓名
        emp_stat,           -- 员工状态
        emp_cat,            -- 员工类别
        hr_emp_cat,         -- 人事系统员工类别
        prnt_mgr_cd,        -- 上级领导工号
        prnt_mgr_nm,        -- 上级领导姓名
        comp_id,            -- 交易归属分公司
        portal_comp_id,     -- 门户所属机构
        hr_comp_id,         -- 人事系统公司ID
        hr_comp_nm,         -- 人事系统公司名称
        hr_dept_nm,         -- 人事系统部门名称
        hr_job_nm,          -- 人事系统岗位名称
        fst_lvl_team_cd,    -- 一级团队负责人工号
        fst_lvl_team_nm,    -- 一级团队负责人
        sec_lvl_team_cd,    -- 二级团队负责人工号
        sec_lvl_team_nm,    -- 二级团队负责人
        join_dt,            -- 入职日期
        left_dt,            -- 离职日期
        cert_type_cd,       -- 证件类型编号
        cert_cd,            -- 证件号码
        hyh_cert_cd,        -- 海银会证件号
        is_had_cust_no,     -- 是否开立交易账户
        map_cust_no,        -- 对应交易账户编号
        emp_job_type,       -- 岗位类别
        emp_email,          -- 员工邮箱
--        appl_left_dt,       -- 申请离职日期
        left_flag,          -- 是否离职
        ofc_phn,            -- 办公室电话
        work_days,          -- 在职天数
        ref_emp_cd          -- 推荐人工号
    )
    SELECT
        TRIM(a.badge)   AS emp_cd,
        TRIM(a.name)    AS emp_nm,
        TRIM(e.title)   AS emp_stat,
        CASE WHEN (TRIM(c.title)='营销中心' AND TRIM(d.title) IN ('上海/福建事业群CEO','副总裁','高级副总裁','首席营销官','助理副总裁')) OR TRIM(d.title)='区域总经理' THEN '前台'
             WHEN COALESCE(portal_comp.comp_id,709)=709 THEN '后台'
             ELSE TRIM(h.title) END AS emp_cat, -- 员工类别(前/后台)
        TRIM(h.title)   AS hr_emp_cat,      -- 人事系统员工类别
        TRIM(t.badge)   AS prnt_mgr_cd,     -- 上级领导工号
        TRIM(t.name)    AS prnt_mgr_nm,     -- 上级领导姓名
        COALESCE(comp.comp_id,709)  AS comp_id,         -- 交易归属分公司
        COALESCE(z.portal_comp_id,709)  AS portal_comp_id,  -- 门户所属机构
        a.compid        AS hr_comp_id,      -- 人事系统公司id
        TRIM(b.title)   AS hr_comp_nm,      -- 人事系统公司名称
        TRIM(c.title)   AS hr_dept_nm,      -- 人事系统部门名称
        TRIM(d.title)   AS hr_job_nm,       -- 人事系统岗位名称
        TRIM(n.badge)   AS fst_lvl_team_cd, -- 一级团队负责人工号
        TRIM(n.name)    AS fst_lvl_team_nm, -- 一级团队负责人
        TRIM(o.badge)   AS sec_lvl_team_cd, -- 二级团队负责人工号
        TRIM(o.name)    AS sec_lvl_team_nm, -- 二级团队负责人
        a.joindate      AS join_dt,
        CASE WHEN TRIM(e.title) IN ('离职','退休') THEN a.leadate END AS left_dt,
        CASE a.certtype
            WHEN 1 THEN '00'
            WHEN 2 THEN '01'
            WHEN 3 THEN '04'
            WHEN 6 THEN '0A'
            ELSE  cast(a.certtype AS varchar) END           AS cert_type_cd,
        upper(TRIM(a.certno))                               AS cert_cd,
        COALESCE(hyh.hyh_cert_code,upper(TRIM(a.certno)))   AS hyh_cert_cd,     -- 海银会证件号 用于海银会业绩匹配
        CASE WHEN t2.cust_no>'' THEN 'Y' ELSE 'N' END       AS is_had_cust_no,  -- 是否开立交易账户
        t2.cust_no                                          AS map_cust_no,     -- 对应交易账户编号
        CASE WHEN vp.vp_cd>'' THEN 'vp' ELSE 'other' END AS emp_job_type,
        a.email,
        -- 批量处理当天是否离职
        CASE WHEN TRIM(e.title) IN ('离职','退休') AND a.leadate<CURRENT_DATE THEN 'Y' ELSE 'N' END AS left_flag,
        a.office_phone,
        COALESCE(CASE WHEN TRIM(e.title) IN ('离职','退休') THEN a.leadate END,CURRENT_DATE)-a.joindate+1 AS work_days,
        TRIM(f.badge)   AS ref_emp_cd
    FROM hq_hr.eemployee            a
    LEFT JOIN hq_hr.ocompany        b ON b.compid=a.compid
    LEFT JOIN hq_hr.odepartment     c ON a.depid=c.depid
    LEFT JOIN hq_hr.ojob            d ON a.jobid=d.jobid
    LEFT JOIN hq_hr.ecd_empstatus   e ON e.id = a.status::varchar
    LEFT JOIN hq_hr.eemployee       n ON a.stairteam=n.eid
    LEFT JOIN hq_hr.eemployee       o ON a.secondteam=o.eid
    LEFT JOIN hq_hr.eemployee       t ON a.reportto=t.eid
    LEFT JOIN hq_hr.ecd_empcategory h ON h.id=a.empcategory
    LEFT JOIN comp.hyh_fp_cert    hyh ON hyh.hyh_cert_code =upper(TRIM(a.certno))
    LEFT JOIN (
        SELECT emp_cd,max(cust_no) cust_no FROM dw.stg_emp_map_to_cust GROUP BY emp_cd
    )                              t2 ON t2.emp_cd=TRIM(a.badge)
    LEFT JOIN (
        SELECT
            e.emp_no AS emp_cd,
            o1.directly_company_id AS portal_comp_id, -- 门户归属机构
            o.directly_company_id AS comp_id -- 门户维护的交易归属部门的直属机构
        FROM portal.hyb_employee e
        LEFT JOIN portal.hyb_organization o ON e.business_org_id=o.id
        LEFT JOIN portal.hyb_organization o1 ON e.org_id=o1.id
    )                               z ON TRIM(a.badge)=z.emp_cd
    LEFT JOIN web.comp_info      comp ON comp.comp_id=z.comp_id -- 交易归属分公司
    LEFT JOIN web.comp_info portal_comp ON portal_comp.comp_id=z.portal_comp_id -- 门户归属分公司
    LEFT JOIN (
        SELECT DISTINCT CASE WHEN length(i.emp_code_vp)>5 THEN TRIM(i.emp_code_vp) ELSE TRIM(i.emp_code_avp) END vp_cd
        FROM web.comp_info i
    )                              vp ON vp.vp_cd=z.emp_cd
    LEFT JOIN hq_hr.eemployee       f ON a.referrer=TRIM(f.badge)   -- 推荐人
    ;

INSERT INTO edw.dim_emp(
        emp_cd,             -- 员工工号
        emp_nm,             -- 姓名
        emp_stat,           -- 员工状态
        emp_cat,            -- 员工类别
        prnt_mgr_cd,        -- 上级领导工号
        prnt_mgr_nm,        -- 上级领导姓名
        comp_id,            -- 交易归属分公司
        portal_comp_id,     -- 门户所属分公司
        hr_comp_id,         -- 人事系统公司ID
        hr_comp_nm,         -- 人事系统公司名称
        hr_dept_nm,         -- 人事系统部门名称
        hr_job_nm,          -- 人事系统岗位名称
        fst_lvl_team_cd,    -- 一级团队负责人工号
        fst_lvl_team_nm,    -- 一级团队负责人
        sec_lvl_team_cd,    -- 二级团队负责人工号
        sec_lvl_team_nm,    -- 二级团队负责人
        join_dt,            -- 入职日期
        left_dt,            -- 离职日期
        cert_type_cd,       -- 证件类型编号
        cert_cd,            -- 证件号码
        hyh_cert_cd,        -- 海银会证件号
        is_had_cust_no,     -- 是否开立交易账户
        map_cust_no,        -- 对应交易账户编号
--        appl_left_dt,       -- 申请离职日期
        left_flag           -- 是否离职
    )
    SELECT
        a.emp_code      AS emp_cd,
        a.emp_name      AS emp_nm,
        '离职'           AS emp_stat,
        a.emp_category  AS emp_cat,
        a.report_to     AS prnt_mgr_cd,  --上级领导工号
        NULL            AS prnt_mgr_nm,  --上级领导姓名
        COALESCE(comp.comp_id,709)          AS comp_id,  --交易归属分公司
        COALESCE(portal_comp.comp_id,709)   AS portal_comp_id,  --门户所属分公司
        a.hr_comp_id    AS hr_comp_id, --人事系统公司id
        a.hr_comp_name  AS hr_comp_nm, --人事系统公司名称
        a.hr_dept_name  AS hr_dept_nm, --人事系统部门名称
        a.hr_job_name   AS hr_job_nm,  --人事系统岗位名称
        a.stair_team    AS fst_lvl_team_cd,  --一级团队负责人工号
        NULL            AS fst_lvl_team_nm,  --一级团队负责人
        a.second_team   AS sec_lvl_team_cd,  --二级团队负责人工号
        NULL            AS sec_lvl_team_nm,  --二级团队负责人
        a.in_date       AS join_dt,
        COALESCE(a.off_date,'2018-08-13')               AS left_dt,
        a.cert_type     AS cert_type_cd,
        a.cert_code     AS cert_cd,
        COALESCE(hyh.hyh_cert_code,a.cert_code)         AS hyh_cert_cd,  --海银会证件号 用于海银会业绩匹配
        CASE WHEN t2.cust_no>'' THEN 'Y' ELSE 'N' END   AS is_had_cust_no,  --是否开立交易账户
        t2.cust_no      AS map_cust_no,    --对应交易账户编号
--        COALESCE(a.off_date,'2018-08-13')               AS appl_left_dt,
        'Y'             AS left_flag
    FROM comp.emp_info_backup a
    LEFT JOIN comp.hyh_fp_cert hyh ON hyh.hyh_cert_code =a.cert_code
    LEFT JOIN dw.stg_emp_map_to_cust t2 ON t2.emp_cd=a.emp_code
    LEFT JOIN dw.dim_emp p ON p.emp_cd=a.emp_code
    LEFT JOIN web.comp_info comp ON a.trans_owner_comp_id=comp.comp_id -- 交易归属
    LEFT JOIN web.comp_info portal_comp ON a.comp_id=portal_comp.comp_id -- 门户归属
    WHERE p.emp_cd IS NULL;
