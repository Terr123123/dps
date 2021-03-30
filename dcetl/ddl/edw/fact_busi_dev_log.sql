CREATE TABLE edw.fact_busi_dev_log (
	id varchar(64) NOT NULL, -- 操作ID
	parent_id varchar(64) NULL, -- 父id
	emp_cd varchar(255) NULL, -- 员工编号
	crt_dtm timestamp NULL, -- 创建时间
	oper_dt date NULL, -- 操作日期
	src_accnt_id varchar(255) NULL, -- 账户ID
	drnt_time int8 NULL, -- 浏览用时
	info_id varchar(60) NULL, --  分享资讯ID 预留
	info_nm varchar(512) NULL, -- 分享标题
	share_id varchar(64) NUll, -- 分享ID
	share_chnl varchar(60) NULL, -- 分享渠道
	action_type varchar(255) NULL, -- 操作类型
	is_share int4 NULL, -- 是否分享
	is_frwrd int4 NULL, -- 是否转发
	is_brws int4 NULL, -- 是否查阅
	CONSTRAINT fact_busi_dev_log_pk PRIMARY KEY (id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_busi_dev_log_oper_dt_idx ON edw.fact_busi_dev_log USING btree (oper_dt) ;
CREATE INDEX fact_busi_dev_log_src_accnt_id_idx ON edw.fact_busi_dev_log USING btree (src_accnt_id) ;

-- Column comments
COMMENT ON TABLE edw.fact_busi_dev_log is '展业内容操作日志';
COMMENT ON COLUMN edw.fact_busi_dev_log.id IS '操作ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.parent_id IS '父id' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.emp_cd IS '员工编号' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.oper_dt IS '操作日期' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.src_accnt_id IS '账户ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.drnt_time IS '浏览用时' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.info_id IS '分享资讯ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.info_nm IS '分享标题' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.share_id IS '分享ID' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.share_chnl IS '分享渠道' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.action_type IS '操作类型' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.is_share IS '是否分享' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.is_frwrd IS '是否转发' ;
COMMENT ON COLUMN edw.fact_busi_dev_log.is_brws IS '是否查阅' ;