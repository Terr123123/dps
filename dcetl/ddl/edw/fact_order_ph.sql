CREATE TABLE edw.fact_order_ph (
	order_cd varchar(50) NULL, -- 订单编号
	trans_cd varchar(50) NULL, -- 交易编号
	cont_cd varchar(50) NULL, -- 合同编号
	src_sys_cd varchar(20) NULL, -- 源系统编码
	prod_id varchar(50) NOT NULL, -- 产品编号
	prod_shr_type_id varchar(20) NULL, -- 份额类别
	prod_open_id varchar(20) NULL, -- 归属开放期ID
	accnt_cd varchar(50) NULL, -- 账户编号
	emp_cd varchar(20) NULL, -- 理财师工号
	comp_id int4 NULL, -- 分公司编号
	cust_no varchar(20) NULL, -- 客户编号
	cont_start_dt date NULL, -- 合同开始日期
	cont_end_dt date NULL, -- 合同结束日期
	intst_start_dt date NULL, -- 起息日期
	entry_dt date NOT NULL, -- 录入日期
	entry_dtm timestamp NULL, -- 录入时间
	cont_amnt numeric(16,2) NULL, -- 合同金额
	conf_stat_cd varchar(10) NULL, -- 审核状态编号
	conf_stat_nm varchar(10) NULL, -- 审核状态名称
	req_stat_cd varchar(10) NULL, -- 申请状态编号
	req_stat_nm varchar(10) NULL, -- 申请状态名称
	conf_stat_flag varchar(1) NOT NULL, -- 确认标识
	is_buy varchar(1) NOT NULL, -- 是否买入
	is_hold varchar(1) NULL,
	is_trnst varchar(1) NULL,
	is_paid varchar(1) NULL,
	norm_std_kpi numeric(6,4) NULL, -- 常规KPI系数
	trans_type_cd varchar(4) NULL, -- 交易类型编号
	trans_type_nm varchar(4) NULL, -- 交易类型名称
	conf_dtm timestamp NULL, -- 总审通过时间
	crcy_type varchar(3) NULL, -- 原币编号
	orgn_crcy_amnt numeric(16,2) NULL -- 原币金额
	,order_crt_dtm timestamp NOT NULL   -- 订单创建时间
	,CONSTRAINT fact_order_ph_pk PRIMARY KEY (order_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_order_ph_comp_id_idx ON edw.fact_order_ph USING btree (comp_id) ;
CREATE INDEX fact_order_ph_cont_cd_idx ON edw.fact_order_ph USING btree (cont_cd) ;
CREATE INDEX fact_order_ph_cust_no_idx ON edw.fact_order_ph USING btree (cust_no) ;
CREATE INDEX fact_order_ph_emp_cd_idx ON edw.fact_order_ph USING btree (emp_cd) ;
CREATE INDEX fact_order_ph_entry_dt_entry_dtm_idx ON edw.fact_order_ph USING btree (entry_dt, entry_dtm) ;
CREATE INDEX fact_order_ph_prod_id_idx ON edw.fact_order_ph USING btree (prod_id) ;
CREATE INDEX fact_order_ph_prod_shr_type_id_idx ON edw.fact_order_ph USING btree (prod_shr_type_id) ;
CREATE INDEX fact_order_ph_trans_cd_idx ON edw.fact_order_ph USING btree (trans_cd) ;

-- Column comments
COMMENT ON TABLE  edw.fact_order_ph IS '普惠订单明细' ;
COMMENT ON COLUMN edw.fact_order_ph.order_cd IS '订单编号' ;
COMMENT ON COLUMN edw.fact_order_ph.trans_cd IS '交易编号' ;
COMMENT ON COLUMN edw.fact_order_ph.cont_cd IS '合同编号' ;
COMMENT ON COLUMN edw.fact_order_ph.src_sys_cd IS '源系统编码' ;
COMMENT ON COLUMN edw.fact_order_ph.prod_id IS '产品编号' ;
COMMENT ON COLUMN edw.fact_order_ph.prod_shr_type_id IS '份额类别' ;
COMMENT ON COLUMN edw.fact_order_ph.prod_open_id IS '归属开放期ID' ;
COMMENT ON COLUMN edw.fact_order_ph.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.fact_order_ph.emp_cd IS '理财师工号' ;
COMMENT ON COLUMN edw.fact_order_ph.comp_id IS '分公司编号' ;
COMMENT ON COLUMN edw.fact_order_ph.cust_no IS '客户编号' ;
COMMENT ON COLUMN edw.fact_order_ph.cont_start_dt IS '合同开始日期' ;
COMMENT ON COLUMN edw.fact_order_ph.cont_end_dt IS '合同结束日期' ;
COMMENT ON COLUMN edw.fact_order_ph.intst_start_dt IS '起息日期' ;
COMMENT ON COLUMN edw.fact_order_ph.entry_dt IS '录入日期' ;
COMMENT ON COLUMN edw.fact_order_ph.entry_dtm IS '录入时间' ;
COMMENT ON COLUMN edw.fact_order_ph.cont_amnt IS '合同金额' ;
COMMENT ON COLUMN edw.fact_order_ph.conf_stat_cd IS '审核状态编号' ;
COMMENT ON COLUMN edw.fact_order_ph.conf_stat_nm IS '审核状态名称' ;
COMMENT ON COLUMN edw.fact_order_ph.req_stat_cd IS '申请状态编号' ;
COMMENT ON COLUMN edw.fact_order_ph.req_stat_nm IS '申请状态名称' ;
COMMENT ON COLUMN edw.fact_order_ph.conf_stat_flag IS '确认标识' ;
COMMENT ON COLUMN edw.fact_order_ph.is_buy IS '是否买入' ;
COMMENT ON COLUMN edw.fact_order_ph.norm_std_kpi IS '常规KPI系数' ;
COMMENT ON COLUMN edw.fact_order_ph.trans_type_cd IS '交易类型编号' ;
COMMENT ON COLUMN edw.fact_order_ph.trans_type_nm IS '交易类型名称' ;
COMMENT ON COLUMN edw.fact_order_ph.conf_dtm IS '总审通过时间' ;
COMMENT ON COLUMN edw.fact_order_ph.crcy_type IS '原币编号' ;
COMMENT ON COLUMN edw.fact_order_ph.orgn_crcy_amnt IS '原币金额' ;
COMMENT ON COLUMN edw.fact_order_ph.order_crt_dtm IS '订单创建时间' ;