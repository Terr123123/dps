CREATE TABLE edw.fact_cust_leads (
	emp_cd varchar(20) NULL, -- 员工编号
	uuid varchar(100) NOT NULL, -- 设备ID或者微信ID
	accnt_cd varchar(50) NULL, -- 账户编号
	leads_crt_dtm timestamp NULL, -- 线索创建时间
	chnl_cd varchar(50) NULL, -- 线索渠道
	CONSTRAINT fact_cust_leads_pk PRIMARY KEY (uuid)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_cust_leads_accnt_cd_idx ON edw.fact_cust_leads USING btree (accnt_cd) ;

-- Column comments
COMMENT ON TABLE edw.fact_cust_leads IS '客户线索事实表';
COMMENT ON COLUMN edw.fact_cust_leads.emp_cd IS '员工编号' ;
COMMENT ON COLUMN edw.fact_cust_leads.uuid IS '设备ID或者微信ID' ;
COMMENT ON COLUMN edw.fact_cust_leads.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.fact_cust_leads.leads_crt_dtm IS '线索创建时间' ;
COMMENT ON COLUMN edw.fact_cust_leads.chnl_cd IS '线索渠道'  ;