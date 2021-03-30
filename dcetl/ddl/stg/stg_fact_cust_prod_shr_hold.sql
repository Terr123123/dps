CREATE TABLE stg.stg_fact_cust_prod_shr_hold (
	accnt_cd varchar(50) NOT NULL, -- 账户编号
	prod_id varchar(20) NOT NULL, -- 产品ID
	prod_shr_type_id varchar(20) NOT NULL, -- 产品份额ID
	tot_shr numeric(12,2) NULL, -- 总份额
	aval_shr numeric(12,2) NULL, -- 可用份额
	frzn_shr numeric(12,2) NULL, -- 冻结份额
	in_transt_amnt numeric(12,2) NULL, -- 在途金额
	net_val numeric(8,4) NULL, -- 单位净值
	bal_amnt numeric(12,2) NULL, -- 余额
	hold_dt date NOT NULL, -- 持有日期
  crt_dtm timestamp NULL, -- 创建时间
	CONSTRAINT his_fact_cust_prod_shr_hold_pk PRIMARY KEY (hold_dt, accnt_cd, prod_id, prod_shr_type_id)
)
WITH (
	OIDS=FALSE
) ;
-- CREATE INDEX stg_fact_cust_prod_shr_hold_accnt_cd_idx ON stg.stg_fact_cust_prod_shr_hold USING btree (accnt_cd) ;
-- CREATE INDEX stg_fact_cust_prod_shr_hold_prod_id_idx ON stg.stg_fact_cust_prod_shr_hold USING btree (prod_id) ;

-- Column comments
COMMENT ON TABLE  stg.stg_fact_cust_prod_shr_hold IS '账户产品持有份额历史表' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.prod_id IS '产品ID' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.prod_shr_type_id IS '产品份额ID' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.tot_shr IS '总份额' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.aval_shr IS '可用份额' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.frzn_shr IS '冻结份额' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.in_transt_amnt IS '在途金额' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.net_val IS '单位净值' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.bal_amnt IS '余额' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.hold_dt IS '持有日期' ;
COMMENT ON COLUMN stg.stg_fact_cust_prod_shr_hold.crt_dtm IS '创建时间' ;