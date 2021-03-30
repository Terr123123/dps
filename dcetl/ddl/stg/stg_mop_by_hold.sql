CREATE TABLE stg.stg_mop_by_hold (
  hold_id serial NOT NULL, -- 持有ID
	order_cd varchar(50) NOT NULL, -- 订单编号
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
	hold_dt date NOT NULL, -- 持有日期
	cont_amnt numeric(16,2) NULL, -- 合同金额
	conf_stat_cd varchar(10) NULL, -- 审核状态编号
	conf_stat_nm varchar(10) NULL, -- 审核状态名称
	req_stat_cd varchar(10) NULL, -- 申请状态编号
	req_stat_nm varchar(10) NULL, -- 申请状态名称
-- 	trans_dt date NULL, -- 交易日期
	conf_stat_flag varchar(1) NOT NULL, -- 确认标识
	norm_std_kpi numeric(8,4) NULL, -- 常规KPI系数
	trans_type_cd varchar(4) NULL, -- 交易类型编号
	trans_type_nm varchar(4) NULL, -- 交易类型名称
	conf_dtm timestamp NULL, -- 总审通过时间
	crcy_type varchar(3) NULL, -- 原币编号
	orgn_crcy_amnt numeric(16,2) NULL, -- 原币金额
	order_crt_dtm timestamp NOT NULL, -- 订单创建时间
	trans_type int4 NULL, -- 交易类型
	std_kpi numeric(8,4) NULL, -- KPI系数
	std_amnt numeric(16,2) NULL -- 业绩金额
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX stg_mop_by_hold_order_cd_idx ON stg.stg_mop_by_hold USING btree (order_cd) ;
CREATE INDEX stg_mop_by_hold_cust_no_idx ON stg.stg_mop_by_hold USING btree (cust_no) ;
-- CREATE INDEX stg_mop_by_hold_emp_cd_idx ON stg.stg_mop_by_hold USING btree (emp_cd) ;
CREATE INDEX stg_mop_by_hold_order_crt_dtm_idx ON stg.stg_mop_by_hold USING btree (order_crt_dtm) ;
-- CREATE INDEX stg_mop_by_hold_prod_id_idx ON stg.stg_mop_by_hold USING btree (prod_id) ;
-- CREATE INDEX stg_mop_by_hold_prod_shr_type_id_idx ON stg.stg_mop_by_hold USING btree (prod_shr_type_id) ;
CREATE INDEX stg_mop_by_hold_hold_dt_idx ON stg.stg_mop_by_hold USING btree (hold_dt) ;

-- Column comments
COMMENT ON COLUMN stg.stg_mop_by_hold IS '保有业绩中间表' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.order_cd IS '订单编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.trans_cd IS '交易编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.cont_cd IS '合同编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.src_sys_cd IS '源系统编码' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.prod_id IS '产品编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.prod_shr_type_id IS '份额类别' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.prod_open_id IS '归属开放期ID' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.emp_cd IS '理财师工号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.comp_id IS '分公司编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.cust_no IS '客户编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.cont_start_dt IS '合同开始日期' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.cont_end_dt IS '合同结束日期' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.intst_start_dt IS '起息日期' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.hold_dt IS '持有日期' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.cont_amnt IS '合同金额' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.conf_stat_cd IS '审核状态编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.conf_stat_nm IS '审核状态名称' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.req_stat_cd IS '申请状态编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.req_stat_nm IS '申请状态名称' ;
-- COMMENT ON COLUMN stg.stg_mop_by_hold.trans_dt IS '交易日期' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.conf_stat_flag IS '确认标识' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.norm_std_kpi IS '常规KPI系数' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.trans_type_cd IS '交易类型编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.trans_type_nm IS '交易类型名称' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.conf_dtm IS '总审通过时间' ;  -- 持有日期
COMMENT ON COLUMN stg.stg_mop_by_hold.crcy_type IS '原币编号' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.orgn_crcy_amnt IS '原币金额' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.order_crt_dtm IS '订单创建时间' ; -- 持有日期
COMMENT ON COLUMN stg.stg_mop_by_hold.trans_type IS '交易类型' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.std_kpi IS 'KPI系数' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.std_amnt IS '业绩金额' ;
COMMENT ON COLUMN stg.stg_mop_by_hold.hold_id IS '持有ID' ;