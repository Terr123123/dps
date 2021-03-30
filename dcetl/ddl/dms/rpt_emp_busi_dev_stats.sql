CREATE TABLE dms.rpt_emp_busi_dev_stats (
	emp_cd varchar(50) NULL, -- 员工编号
	oper_dt date NULL, -- 统计日期
	share_chnl varchar(60) NULL, -- 渠道编码
	leads_cnt int4 NULL, -- 新增线索数
	share_cnt int4 NULL, -- 分享次数
	frwrd_cnt int4 NULL, -- 被转发数
	brws_cnt int4 NULL, -- 浏览次数
	vstr_cnt int4 NULL -- 浏览人数
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX rpt_emp_busi_dev_stats_emp_cd_idx ON dms.rpt_emp_busi_dev_stats USING btree (emp_cd) ;
CREATE INDEX rpt_emp_busi_dev_stats_oper_dt_idx ON dms.rpt_emp_busi_dev_stats USING btree (oper_dt) ;

-- Column comments
COMMENT ON TABLE  dms.rpt_emp_busi_dev_stats IS '理财师展业统计' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.emp_cd IS '员工编号' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.oper_dt IS '统计日期' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.share_chnl IS '渠道编码' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.leads_cnt IS '新增线索数' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.share_cnt IS '分享次数' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.frwrd_cnt IS '被转发数' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.brws_cnt IS '浏览次数' ;
COMMENT ON COLUMN dms.rpt_emp_busi_dev_stats.vstr_cnt IS '浏览人数' ;