CREATE TABLE stg.stg_busi_dev_emp_used (
	emp_cd varchar(50) NOT NULL, -- 所属人
	start_used_dt date NULL, -- 开始使用日期
	CONSTRAINT stg_busi_dev_emp_used_pk PRIMARY KEY (emp_cd)
)
WITH (
	OIDS=FALSE
) ;

-- Column comments
COMMENT ON TABLE stg.stg_busi_dev_emp_used IS '员工开始使用记录' ;
COMMENT ON COLUMN stg.stg_busi_dev_emp_used.emp_cd IS '所属人' ;
COMMENT ON COLUMN stg.stg_busi_dev_emp_used.start_used_dt IS '开始使用日期' ;