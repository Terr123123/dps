CREATE TABLE edw.fact_busi_dev_leads (
	src_accnt_id varchar(60) NOT NULL, -- 员工编号
	emp_cd varchar(50)  NULL, -- 员工编号
	crt_dtm timestamp NULL, -- 线索创建时间
	oper_dt date NULL, -- 操作日期
	share_id varchar(60) NULL, -- 来源的分享ID
	share_chnl varchar(60) NULL, -- 分享渠道
	CONSTRAINT fact_busi_dev_leads_pk PRIMARY KEY (src_accnt_id,emp_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_busi_dev_leads_crt_dtm_idx ON edw.fact_busi_dev_leads USING btree (crt_dtm) ;
CREATE INDEX fact_busi_dev_leads_oper_dt_idx ON edw.fact_busi_dev_leads USING btree (oper_dt) ;

-- Column comments
COMMENT ON TABLE edw.fact_busi_dev_leads IS '客户线索事实表';
COMMENT ON COLUMN edw.fact_busi_dev_leads.emp_cd IS '员工编号' ;
COMMENT ON COLUMN edw.fact_busi_dev_leads.src_accnt_id IS '源系统账户编号' ;
COMMENT ON COLUMN edw.fact_busi_dev_leads.oper_dt IS '操作日期' ;
COMMENT ON COLUMN edw.fact_busi_dev_leads.crt_dtm IS '线索创建时间' ;
COMMENT ON COLUMN edw.fact_busi_dev_leads.share_id IS '来源的分享ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_leads.share_chnl IS '分享渠道' ;