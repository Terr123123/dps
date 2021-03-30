CREATE TABLE stg.stg_busi_dev_share (
	share_id varchar(20) NOT NULL, -- 分享ID
	emp_cd varchar NULL, -- 员工编号
	share_dtm timestamp NULL, -- 分享时间
	share_chnl varchar(20) NULL, -- 分享渠道
	info_id varchar(50) NULL, -- 资讯ID
	info_nm  varchar(200) null , -- 资讯名称
	data_from varchar(20) NULL, -- 系统来源
--	share_from_who varchar(10) NULL, -- 分享来自谁 fp 表示理财师 emp表示非理财师员工 cust 表示客户
	CONSTRAINT stg_busi_dev_share_pk PRIMARY KEY (share_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX stg_busi_dev_share_share_dtm_idx ON stg.stg_busi_dev_share USING btree (share_dtm) ;

COMMENT ON TABLE  stg.stg_busi_dev_share IS '展业分享明细中间表';
COMMENT ON COLUMN stg.stg_busi_dev_share.share_id IS '分享ID' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.emp_cd IS '员工编号' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.share_dtm IS '分享时间' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.share_chnl IS '分享渠道' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.info_id IS '资讯ID' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.info_nm IS '资讯名称' ;
COMMENT ON COLUMN stg.stg_busi_dev_share.data_from IS '系统来源' ;
-- COMMENT ON COLUMN stg.stg_busi_dev_share.share_from_who IS '分享来自谁' ; -- 分享来自谁 fp 表示理财师 emp表示非理财师员工 cust 表示客户