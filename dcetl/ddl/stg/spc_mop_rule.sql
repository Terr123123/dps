CREATE TABLE stg.spc_mop_rule (
  rule_id varchar(20) NOT NULL, -- 规则ID  YYYYMMDD+当天的一个编号1\2\3\4\5
	trans_cd varchar(50) NULL, -- 交易编号
	cont_cd varchar(50) NULL, -- 合同编号
	src_sys_cd varchar(20) NULL, -- 源系统编码
	prod_id varchar(50) NULL, -- 产品编号
	prod_shr_type_id varchar(20) NULL, -- 份额类别
	accnt_cd varchar(50) NULL, -- 账户编号
	emp_cd varchar(20) NULL, -- 理财师工号
	comp_id int4 NULL, -- 分公司编号
	cust_no varchar(20) NULL, -- 客户编号
	adj_std_kpi numeric(6,4) NULL DEFAULT 0, -- 调整系数，调整系数 不为0时，adj_std_amt_by_ratio 必须为0 不会会新增一条虚拟单据
	adj_std_amnt numeric(16,2) NULL DEFAULT 0, -- 调整业绩金额  会新增一条虚拟单据
	adj_std_amnt_by_ratio numeric(8,4) NULL DEFAULT 0, -- 按比例调整业绩金额 会新增一条虚拟单据
	start_trans_dt date NOT NULL DEFAULT '2000-01-01'::date, -- 交易开始日期
	end_trans_dt date NOT NULL DEFAULT '9999-12-31'::date, -- 交易结束日期
	adj_rmk text NULL, -- 调整说明
  rule_add_dt date NULL, -- 规则添加日期 有业务含义的，例如红冲或不发业绩的，这个日期是业务调整日期
	CONSTRAINT spc_mop_rule_pk PRIMARY KEY (rule_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX spc_mop_rule_comp_id_idx ON stg.spc_mop_rule USING btree (comp_id) ;
CREATE INDEX spc_mop_rule_cont_cd_idx ON stg.spc_mop_rule USING btree (cont_cd) ;
CREATE INDEX spc_mop_rule_cust_no_idx ON stg.spc_mop_rule USING btree (cust_no) ;
CREATE INDEX spc_mop_rule_emp_cd_idx ON stg.spc_mop_rule USING btree (emp_cd) ;
CREATE INDEX spc_mop_rule_prod_id_idx ON stg.spc_mop_rule USING btree (prod_id) ;
CREATE INDEX spc_mop_rule_prod_shr_type_id_idx ON stg.spc_mop_rule USING btree (prod_shr_type_id) ;
CREATE INDEX spc_mop_rule_trans_cd_idx ON stg.spc_mop_rule USING btree (trans_cd) ;

-- Column comments
COMMENT ON TABLE stg.spc_mop_rule IS '业绩规则调整表' ;
COMMENT ON COLUMN stg.spc_mop_rule.trans_cd IS '交易编号' ;
COMMENT ON COLUMN stg.spc_mop_rule.cont_cd IS '合同编号' ;
COMMENT ON COLUMN stg.spc_mop_rule.src_sys_cd IS '源系统编码' ;
COMMENT ON COLUMN stg.spc_mop_rule.prod_id IS '产品编号' ;
COMMENT ON COLUMN stg.spc_mop_rule.prod_shr_type_id IS '份额类别' ;
COMMENT ON COLUMN stg.spc_mop_rule.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN stg.spc_mop_rule.emp_cd IS '理财师工号' ;
COMMENT ON COLUMN stg.spc_mop_rule.comp_id IS '分公司编号' ;
COMMENT ON COLUMN stg.spc_mop_rule.cust_no IS '客户编号' ;
-- COMMENT ON COLUMN stg.spc_mop_rule.trans_dt IS '交易日期' ;
COMMENT ON COLUMN stg.spc_mop_rule.adj_std_kpi IS '调整系数' ;
COMMENT ON COLUMN stg.spc_mop_rule.adj_std_amnt IS '调整业绩金额' ;
COMMENT ON COLUMN stg.spc_mop_rule.adj_std_amnt_by_ratio IS '调整合同金额' ;
COMMENT ON COLUMN stg.spc_mop_rule.start_trans_dt IS '交易开始日期' ;
COMMENT ON COLUMN stg.spc_mop_rule.end_trans_dt IS '交易结束日期' ;
COMMENT ON COLUMN stg.spc_mop_rule.adj_rmk IS '调整说明' ;
COMMENT ON COLUMN stg.spc_mop_rule.rule_id IS '规则ID' ;
COMMENT ON COLUMN stg.spc_mop_rule.rule_add_dt IS '规则添加日期' ;