CREATE TABLE edw.fact_emp_busi_dev_stats (
	emp_cd varchar(20) NOT NULL, -- 员工号
	oper_dt date NOT NULL, -- 拓展日期
	leads_cnt int8 NULL, -- 新增销售线索
	shr_cnt int8 NULL, -- 分享次数
	brs_cnt int8 NULL, -- 浏览次数
	frwrd_cnt int8 NULL, -- 转发次数
	vstr_cnt int8 NULL, -- 访客人数
	chnl_cd varchar(20) NOT NULL, -- 拓展渠道
	CONSTRAINT fact_emp_busi_dev_stats_pk PRIMARY KEY (oper_dt, emp_cd, chnl_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_emp_busi_dev_stats_emp_cd_idx ON edw.fact_emp_busi_dev_stats USING btree (emp_cd) ;

-- Column comments
COMMENT ON TABLE edw.fact_emp_busi_dev_stats IS '员工拓展客户统计表' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.emp_cd IS '员工号' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.oper_dt IS '拓展日期' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.leads_cnt IS '新增销售线索' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.shr_cnt IS '分享次数' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.brs_cnt IS '浏览次数' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.frwrd_cnt IS '转发次数' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.vstr_cnt IS '访客人数' ;
COMMENT ON COLUMN edw.fact_emp_busi_dev_stats.chnl_cd IS '拓展渠道' ;