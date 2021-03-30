CREATE TABLE edw.link_accnt_cust_no (
	auto_id serial NOT NULL, -- 自增ID
	accnt_cd varchar(50) NOT NULL, -- 账户编号
	cust_no int4 NOT NULL, -- 客户编号
	start_dt date NOT NULL, -- 有效开始日期
	end_dt date NOT NULL, -- 有效结束日期
	crt_dtm timestamp NOT NULL DEFAULT now(), -- 创建时间
	upd_dtm timestamp NULL, -- 更新时间
	CONSTRAINT link_accnt_cust_no_pk PRIMARY KEY (accnt_cd, cust_no, start_dt)
)
WITH (
	OIDS=TRUE
) ;
CREATE unique index link_accnt_cust_no_auto_id_idx ON edw.link_accnt_cust_no USING btree (auto_id) ;
CREATE INDEX link_accnt_cust_no_cust_no_idx ON edw.link_accnt_cust_no USING btree (cust_no) ;
CREATE INDEX link_accnt_cust_no_end_dt_idx ON edw.link_accnt_cust_no USING btree (end_dt) ;
CREATE INDEX link_accnt_cust_no_start_dt_idx ON edw.link_accnt_cust_no USING btree (start_dt) ;

-- Column comments
COMMENT ON table  edw.link_accnt_cust_no IS '账户客户编号变化拉链表';
COMMENT ON COLUMN edw.link_accnt_cust_no.auto_id IS '自增ID' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.cust_no IS '客户编号' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.start_dt IS '有效开始日期' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.end_dt IS '有效结束日期' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.link_accnt_cust_no.upd_dtm IS '更新时间' ;
