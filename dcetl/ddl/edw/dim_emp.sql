CREATE TABLE edw.dim_emp (
        emp_cd varchar(30) NOT NULL, -- 员工工号
        emp_nm varchar(50) NULL,    -- 姓名
        emp_stat varchar(20) NULL,  -- 员工状态
        emp_cat varchar(20) NULL,   -- 员工类别
        prnt_mgr_cd varchar(30) NULL, -- 上级领导工号
        prnt_mgr_nm varchar(50) NULL, -- 上级领导姓名
        comp_id int8 NULL,  -- 交易归属分公司
        portal_comp_id int8 NULL,       -- 门户所属分公司
        hr_comp_id varchar(10) NULL,    -- 人事系统公司ID
        hr_comp_nm varchar(100) NULL,   -- 人事系统公司名称
        hr_dept_nm varchar(100) NULL,   -- 人事系统部门名称
        hr_job_nm varchar(100) NULL,    -- 人事系统岗位名称
        fst_lvl_team_cd varchar(30) NULL, -- 一级团队负责人工号
        fst_lvl_team_nm varchar(50) NULL, -- 一级团队负责人
        sec_lvl_team_cd varchar(30) NULL, -- 二级团队负责人工号
        sec_lvl_team_nm varchar(50) NULL, -- 二级团队负责人
        join_dt date NULL, -- 入职日期
        left_dt date NULL, -- 离职日期
        cert_type_cd varchar(2) NULL,   -- 证件类型编号
        cert_cd varchar(50) NULL,      -- 证件号码
        hyh_cert_cd varchar(128) NULL,  -- 海银会证件号
        is_had_cust_no varchar(2) NULL, -- 是否开立交易账户
        map_cust_no varchar(30) NULL,   -- 对应交易账户编号
        emp_email varchar(60) NULL,     -- 邮箱地址
        emp_job_type varchar(10) Null default 'other', -- 岗位类别 vp other
--         appl_left_dt date NULL, -- 申请离职日期(作废)
        left_flag varchar(1),   -- 是否离职(Y:是 N:否)
        ofc_phn varchar(50),    -- 办公室电话
        work_days int4,         -- 在职天数
        hr_emp_cat varchar(20) NULL,    -- 人事系统员工类别
        ref_emp_cd varchar(12) NULL,    -- 推荐人工号
        CONSTRAINT dim_emp_pk PRIMARY KEY (emp_cd)
)WITH (
        OIDS=FALSE
) ;
    CREATE INDEX dim_emp_comp_id_idx ON edw.dim_emp USING btree (comp_id) ;
    COMMENT ON TABLE edw.dim_emp IS '员工信息' ;
    COMMENT ON COLUMN edw.dim_emp.emp_cd IS '员工工号' ;
    COMMENT ON COLUMN edw.dim_emp.emp_nm IS '姓名' ;
    COMMENT ON COLUMN edw.dim_emp.emp_stat IS '员工状态' ;
    COMMENT ON COLUMN edw.dim_emp.emp_cat IS '员工类别' ;
    COMMENT ON COLUMN edw.dim_emp.prnt_mgr_cd IS '上级领导工号' ;
    COMMENT ON COLUMN edw.dim_emp.prnt_mgr_nm IS '上级领导姓名' ;
    COMMENT ON COLUMN edw.dim_emp.comp_id IS '交易归属分公司' ;
    COMMENT ON COLUMN edw.dim_emp.portal_comp_id IS '门户所属分公司' ;
    COMMENT ON COLUMN edw.dim_emp.hr_comp_id IS '人事系统公司ID' ;
    COMMENT ON COLUMN edw.dim_emp.hr_comp_nm IS '人事系统公司名称' ;
    COMMENT ON COLUMN edw.dim_emp.hr_dept_nm IS '人事系统部门名称' ;
    COMMENT ON COLUMN edw.dim_emp.hr_job_nm IS '人事系统岗位名称' ;
    COMMENT ON COLUMN edw.dim_emp.fst_lvl_team_cd IS '一级团队负责人工号' ;
    COMMENT ON COLUMN edw.dim_emp.fst_lvl_team_nm IS '一级团队负责人' ;
    COMMENT ON COLUMN edw.dim_emp.sec_lvl_team_cd IS '二级团队负责人工号' ;
    COMMENT ON COLUMN edw.dim_emp.sec_lvl_team_nm IS '二级团队负责人' ;
    COMMENT ON COLUMN edw.dim_emp.join_dt IS '入职日期' ;
    COMMENT ON COLUMN edw.dim_emp.left_dt IS '离职日期' ;
    COMMENT ON COLUMN edw.dim_emp.cert_type_cd IS '证件类型编号' ;
    COMMENT ON COLUMN edw.dim_emp.cert_cd IS '证件号码' ;
    COMMENT ON COLUMN edw.dim_emp.hyh_cert_cd IS '海银会证件号' ;
    COMMENT ON COLUMN edw.dim_emp.is_had_cust_no IS '是否开立交易账户' ;
    COMMENT ON COLUMN edw.dim_emp.map_cust_no IS '对应交易账户编号' ;
    COMMENT ON COLUMN edw.dim_emp.emp_job_type IS '岗位类别' ; -- vp other
    COMMENT ON COLUMN edw.dim_emp.emp_email is '员工邮箱';
    COMMENT ON COLUMN edw.dim_emp.left_flag IS '是否离职(Y:是 N:否)';
    COMMENT ON COLUMN edw.dim_emp.ofc_phn IS '办公室电话';
    COMMENT ON COLUMN edw.dim_emp.work_days IS '在职天数';
    COMMENT ON COLUMN edw.dim_emp.hr_emp_cat IS '人事系统员工类别' ;
    COMMENT ON COLUMN edw.dim_emp.ref_emp_cd IS '推荐人工号' ;