CREATE TABLE edw.fact_cust_pay (
	paid_no varchar(30) NOT NULL, -- 付款编号
	src_sys_cd varchar(30) NULL, -- 源系统编码
	req_seq varchar(30) NULL, -- 申请编号
	mgr_capi_seq_id int4 NULL, -- 管理人流水ID
	emp_cd varchar(20) NULL, -- 理财师编号
	comp_id int4 NULL, -- 分公司
	paid_dt date NULL, -- 付款日期
	paid_amnt numeric(20,2) NULL, -- 付款金额
	pay_arrv_dt date NULL, -- 到账日期
	src_order_no varchar(50) NULL, -- 预约单号
	pay_arrv_amnt numeric(16,2) NULL, -- 到账金额
	accnt_cd varchar(30) NULL, -- 账户编号
	cust_no varchar(30) NULL, -- 客户编号
	is_valid varchar(2) NULL, -- 是否有效付款
	mgr_acct_cd varchar(50) NULL, -- 管理人编号
	is_arrv varchar(2) NULL, -- 是否到账
	paid_crt_dtm timestamp NULL, -- 付款单创建时间
	CONSTRAINT fact_cust_pay_pk PRIMARY KEY (paid_no)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_cust_pay_cust_no_idx ON edw.fact_cust_pay USING btree (cust_no) ;
CREATE INDEX fact_cust_pay_emp_cd_idx ON edw.fact_cust_pay USING btree (emp_cd) ;
CREATE INDEX fact_cust_pay_paid_dt_idx ON edw.fact_cust_pay USING btree (paid_dt) ;
CREATE INDEX fact_cust_pay_pay_arrv_dt_idx ON edw.fact_cust_pay USING btree (pay_arrv_dt) ;
CREATE INDEX fact_cust_pay_req_seq_idx ON edw.fact_cust_pay USING btree (req_seq) ;

-- Column comments
COMMENT ON TABLE  edw.fact_cust_pay IS '客户付款事实表';
COMMENT ON COLUMN edw.fact_cust_pay.paid_no IS '付款编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.src_sys_cd IS '源系统编码' ;
COMMENT ON COLUMN edw.fact_cust_pay.req_seq IS '申请编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.mgr_capi_seq_id IS '管理人流水ID' ;
COMMENT ON COLUMN edw.fact_cust_pay.emp_cd IS '理财师编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.comp_id IS '分公司' ;
COMMENT ON COLUMN edw.fact_cust_pay.paid_dt IS '付款日期' ;
COMMENT ON COLUMN edw.fact_cust_pay.paid_amnt IS '付款金额' ;
COMMENT ON COLUMN edw.fact_cust_pay.pay_arrv_dt IS '到账日期' ;
COMMENT ON COLUMN edw.fact_cust_pay.src_order_no IS '预约单号' ;
COMMENT ON COLUMN edw.fact_cust_pay.pay_arrv_amnt IS '到账金额' ;
COMMENT ON COLUMN edw.fact_cust_pay.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.cust_no IS '客户编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.is_valid IS '是否有效付款' ;
COMMENT ON COLUMN edw.fact_cust_pay.mgr_acct_cd IS '管理人编号' ;
COMMENT ON COLUMN edw.fact_cust_pay.is_arrv IS '是否到账' ;
COMMENT ON COLUMN edw.fact_cust_pay.paid_crt_dtm IS '付款单创建时间' ;