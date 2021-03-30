CREATE TABLE edw.link_cust_owner (
	cust_no int4 NOT NULL, -- 客户编号
	comp_id int4 NOT NULL, -- 分公司
	emp_cd varchar(50) NULL, -- 理财师编号
	start_dt date NOT NULL, -- 有效开始日期
	end_dt date NOT NULL, -- 有效结束日期
	crt_dtm timestamp NULL DEFAULT now(), -- 创建时间
	upd_dtm timestamp NULL DEFAULT now(), -- 更新时间
	auto_id serial NOT NULL,
	CONSTRAINT link_cust_owner_pk PRIMARY KEY (cust_no, start_dt)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX link_cust_owner_auto_id_idx ON edw.link_cust_owner USING btree (auto_id) ;
CREATE INDEX link_cust_owner_end_dt_idx ON edw.link_cust_owner USING btree (end_dt) ;

-- Column comments
COMMENT ON TABLE  edw.link_cust_owner IS '客户归属拉链表';
COMMENT ON COLUMN edw.link_cust_owner.cust_no IS '客户编号' ;
COMMENT ON COLUMN edw.link_cust_owner.comp_id IS '分公司' ;
COMMENT ON COLUMN edw.link_cust_owner.emp_cd IS '理财师编号' ;
COMMENT ON COLUMN edw.link_cust_owner.start_dt IS '有效开始日期' ;
COMMENT ON COLUMN edw.link_cust_owner.end_dt IS '有效结束日期' ;
COMMENT ON COLUMN edw.link_cust_owner.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.link_cust_owner.upd_dtm IS '更新时间' ;