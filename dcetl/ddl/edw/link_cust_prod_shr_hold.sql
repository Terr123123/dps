CREATE TABLE edw.link_cust_prod_shr_hold (
  auto_id serial8 NOT NULL, -- 自增ID
	accnt_cd varchar(50) NOT NULL, -- 账户编号
	prod_id varchar(20) NOT NULL, -- 产品ID
	prod_shr_type_id varchar(20) NOT NULL, -- 产品份额ID
	bal_amnt numeric(12,2) NULL, -- 余额
  start_dt date NOT NULL, -- 开始日期
	end_dt date NOT NULL, -- 结束日期
  crt_dtm timestamp NULL, -- 创建时间
  upd_dtm timestamp NULL, -- 更新时间
	CONSTRAINT link_cust_prod_shr_hold_pk PRIMARY KEY (accnt_cd, prod_id, prod_shr_type_id,start_dt)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX link_cust_prod_shr_hold_prod_id_idx ON edw.link_cust_prod_shr_hold USING btree (prod_id) ;
CREATE INDEX link_cust_prod_shr_hold_dt_idx ON edw.link_cust_prod_shr_hold USING btree (start_dt,end_dt) ;

-- Column comments
COMMENT ON TABLE  edw.link_cust_prod_shr_hold IS '账户产品持有份额拉链表' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.auto_id IS '自增ID' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.prod_id IS '产品ID' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.prod_shr_type_id IS '产品份额ID' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.start_dt IS '开始日期' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.end_dt IS '结束日期' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.bal_amnt IS '余额' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.link_cust_prod_shr_hold.upd_dtm IS '更新时间' ;