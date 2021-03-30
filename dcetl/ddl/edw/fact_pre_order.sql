CREATE TABLE edw.fact_pre_order (
	order_no varchar(50) NOT NULL, -- 预约唯一标识
	src_order_no varchar(50) NULL, -- 预约编号
	emp_cd varchar(8) NULL, -- 理财师工号
	order_dtm timestamp NULL, -- 预约时间
	comp_id int4 NULL, -- 公司ID
	accnt_cd varchar(20) NULL, -- 账户信息
  cust_no varchar(20) NULL, -- 账户编号
	prod_id varchar(20) NULL, -- 产品ID
	shr_type_id varchar(20) NULL, -- 产品份额ID
	order_amnt numeric(16,2) NULL, -- 预约金额
	chk_stat_id varchar(2) NULL, -- 审核状态码
	chh_stat_nm varchar(100) NULL, -- 审核状态
	used_stat_id varchar(2) NULL, -- 使用状态码
	used_stat_nm varchar(100) NULL, -- 使用状态
	conf_stat_id varchar(2) NULL, -- 确认状态码
	conf_stat_nm varchar(100) NULL, -- 确认状态
	order_cont_no varchar(20) NULL, -- 预约合同号
	is_valid_order varchar(1) NULL, -- 是否有效预约
	CONSTRAINT fact_pre_order_pk PRIMARY KEY (order_no)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_pre_order_accnt_cd_idx ON edw.fact_pre_order USING btree (accnt_cd) ;
CREATE INDEX fact_pre_order_cust_no_idx ON edw.fact_pre_order USING btree (cust_no) ;
CREATE INDEX fact_pre_order_order_dtm_idx ON edw.fact_pre_order USING btree (order_dtm) ;
CREATE INDEX fact_pre_order_src_order_no_idx ON edw.fact_pre_order USING btree (src_order_no) ;

-- Column comments
COMMENT ON TABLE  edw.fact_pre_order IS '预约事实表';
COMMENT ON COLUMN edw.fact_pre_order.order_no IS '预约唯一标识' ;
COMMENT ON COLUMN edw.fact_pre_order.src_order_no IS '预约编号' ;
COMMENT ON COLUMN edw.fact_pre_order.emp_cd IS '理财师工号' ;
COMMENT ON COLUMN edw.fact_pre_order.order_dtm IS '预约时间' ;
COMMENT ON COLUMN edw.fact_pre_order.comp_id IS '公司ID' ;
COMMENT ON COLUMN edw.fact_pre_order.accnt_cd IS '账户信息' ;
COMMENT ON COLUMN edw.fact_pre_order.cust_no IS '账户编号' ;
COMMENT ON COLUMN edw.fact_pre_order.prod_id IS '产品ID' ;
COMMENT ON COLUMN edw.fact_pre_order.shr_type_id IS '产品份额ID' ;
COMMENT ON COLUMN edw.fact_pre_order.order_amnt IS '预约金额' ;
COMMENT ON COLUMN edw.fact_pre_order.chk_stat_id IS '审核状态码' ;
COMMENT ON COLUMN edw.fact_pre_order.chh_stat_nm IS '审核状态' ;
COMMENT ON COLUMN edw.fact_pre_order.used_stat_id IS '使用状态码' ;
COMMENT ON COLUMN edw.fact_pre_order.used_stat_nm IS '使用状态' ;
COMMENT ON COLUMN edw.fact_pre_order.conf_stat_id IS '确认状态码' ;
COMMENT ON COLUMN edw.fact_pre_order.conf_stat_nm IS '确认状态' ;
COMMENT ON COLUMN edw.fact_pre_order.order_cont_no IS '预约合同号' ;
COMMENT ON COLUMN edw.fact_pre_order.is_valid_order IS '是否有效预约' ;
