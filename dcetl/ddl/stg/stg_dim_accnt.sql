CREATE TABLE stg.stg_dim_accnt (
	accnt_cd varchar(50) NOT NULL, -- 账号编号
	src_sys_cust_id varchar(50) NULL, -- 源系统账号ID
	src_sys_cust_cd varchar(50) NULL, -- 源系统账号编号
	src_sys_cd varchar(10) not null , -- 源系统编号
	cust_nm varchar(100) NULL, -- 客户名称
	is_org varchar(2) NULL, -- 是否机构
	emp_cd varchar(10) NULL, -- 归属理财师
	mbr_no varchar(50) NULL, -- 会员编号
	mbr_stat varchar(20) NULL, -- 会员状态
	admit_dt date NULL, -- 入会日期
	cert_type varchar(2) NULL, -- 证件类型
	cert_cd varchar(50) NULL, -- 证件号
	is_secret_to_fmly varchar(2) NULL, -- 是否对家庭保密
	cust_from varchar NULL, -- 客户来源
	curr_cust_grade varchar NULL, -- 当前客户等级
	cust_risk_lvl varchar(100) NULL, -- 风险等级
	cust_sex varchar(2) NULL, -- 客户性别
	crt_dtm timestamp NULL, -- 创建时间
	cust_curr_stat varchar(10) NULL, -- 客户当前状态
	is_emp varchar(2) NULL, -- 是否员工
	cust_bday date ,-- 客户生日
	CONSTRAINT stg_dim_accnt_pk PRIMARY KEY (accnt_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX stg_dim_accnt_cert_cd_idx ON stg.stg_dim_accnt USING btree (cert_cd) ;
-- Column comments
comment on table  stg.stg_dim_accnt IS '账户信息中间表';
COMMENT ON COLUMN stg.stg_dim_accnt.accnt_cd IS '账号编号' ;
COMMENT ON COLUMN stg.stg_dim_accnt.src_sys_cust_id IS '源系统账户ID' ;
COMMENT ON COLUMN stg.stg_dim_accnt.src_sys_cust_cd IS '源系统账户编号' ;
COMMENT ON COLUMN stg.stg_dim_accnt.src_sys_cd IS '源系统编号' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_nm IS '客户名称' ;
COMMENT ON COLUMN stg.stg_dim_accnt.is_org IS '是否机构' ;
COMMENT ON COLUMN stg.stg_dim_accnt.emp_cd IS '归属理财师' ;
COMMENT ON COLUMN stg.stg_dim_accnt.mbr_no IS '会员编号' ;
COMMENT ON COLUMN stg.stg_dim_accnt.mbr_stat IS '会员状态' ;
COMMENT ON COLUMN stg.stg_dim_accnt.admit_dt IS '入会日期' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cert_type IS '证件类型' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cert_cd IS '证件号' ;
COMMENT ON COLUMN stg.stg_dim_accnt.is_secret_to_fmly IS '是否对家庭保密' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_from IS '客户来源' ;
COMMENT ON COLUMN stg.stg_dim_accnt.curr_cust_grade IS '当前客户等级' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_risk_lvl IS '客户风险等级' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_sex IS '客户性别' ;
COMMENT ON COLUMN stg.stg_dim_accnt.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_curr_stat IS '客户当前状态' ;
COMMENT ON COLUMN stg.stg_dim_accnt.is_emp IS '是否员工' ;
COMMENT ON COLUMN stg.stg_dim_accnt.cust_bday IS '客户生日' ;
