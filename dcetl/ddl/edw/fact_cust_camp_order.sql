CREATE TABLE edw.fact_cust_camp_order (
	camp_cd varchar(50) NULL, -- 活动编码
	src_camp_id int4 NOT NULL, -- 源系统活动ID
	accnt_cd varchar(50) NOT NULL, -- 账户编码
	emp_cd varchar(8) NULL, -- 理财师编码
	is_new_cust varchar(1) NULL, -- 是否新客
	camp_start_dt date NULL, -- 活动开始日期
	src_sys_cd varchar(20) NOT NULL, -- 源系统编码
	esti_sign_amnt numeric NULL, -- 预计签单金额
	arrv_flag int4 NULL, -- 是否到场
	CONSTRAINT fact_cust_camp_order_pk PRIMARY KEY (src_camp_id, accnt_cd, src_sys_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_cust_camp_order_accnt_cd_idx ON edw.fact_cust_camp_order USING btree (accnt_cd) ;
CREATE INDEX fact_cust_camp_order_emp_cd_idx ON edw.fact_cust_camp_order USING btree (emp_cd) ;

-- Column comments
COMMENT ON TABLE edw.fact_cust_camp_order IS '客户活动预约' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.camp_cd IS '活动编码' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.src_camp_id IS '源系统活动ID' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.accnt_cd IS '账户编码' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.emp_cd IS '理财师编码' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.is_new_cust IS '是否新客' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.camp_start_dt IS '活动开始日期' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.src_sys_cd IS '源系统编码' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.esti_sign_amnt IS '预计签单金额' ;
COMMENT ON COLUMN edw.fact_cust_camp_order.arrv_flag IS '是否到场' ;