 CREATE TABLE edw.fact_busi_dev_emp_acc_used (
	emp_cd varchar(30) NOT NULL, -- 员工号
	emp_nm varchar(50) NULL, -- 员工名称
	hr_job_nm varchar(100) NULL, -- 人事岗位名称
	join_dt date NULL, -- 入职日期
	start_used_dt date NULL, -- 开始使用日期
	comp_id int8 NULL, -- 分公司ID
	emp_stat varchar(20) NULL, -- 在职状态
	used_flag int4 NULL, -- 使用标识
	if_check_kpi int4 NULL, -- 是否考核KPI
	check_ym varchar(10) NOT NULL, -- 考核年月
	CONSTRAINT fact_busi_dev_emp_acc_used_pk PRIMARY KEY (check_ym, emp_cd)
)
WITH (
	OIDS=FALSE
) ;

-- Column comments
COMMENT ON TABLE  edw.fact_busi_dev_emp_acc_used IS '理财师展业累计使用明细' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.emp_cd IS '员工号' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.emp_nm IS '员工名称' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.hr_job_nm IS '人事岗位名称' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.join_dt IS '入职日期' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.start_used_dt IS '开始使用日期' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.comp_id IS '分公司ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.emp_stat IS '在职状态' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.used_flag IS '使用标识' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.if_check_kpi IS '是否考核KPI' ;
COMMENT ON COLUMN edw.fact_busi_dev_emp_acc_used.check_ym IS '考核年月' ;

