CREATE TABLE adl.mapp_comp_busi_dev_stats (
	comp_id int NOT NULL, -- 理财师编号
	comp_shrt_nm varchar(100)   NULL, -- 分公司简称
	prd_type varchar(20) NOT NULL, -- 期间类别
	shr_cnt int NULL DEFAULT 0, -- 分享次数
	frwrd_cnt int NULL DEFAULT 0, -- 被转发数
	vstr_cnt int NULL DEFAULT 0, -- 访客数量
	brs_cnt int NULL DEFAULT 0, -- 浏览数量
	camp_invt_cust_cnt int NULL DEFAULT 0, -- 线下活动邀请人数
	camp_cust_arrv_cnt int NULL DEFAULT 0, -- 线下到达到场人数
	camp_cnt int NULL DEFAULT 0, -- 预约客户数
	rpt_dtm timestamp DEFAULT now() , -- 报表时间
	CONSTRAINT mapp_comp_busi_dev_stats_pk PRIMARY KEY (comp_id, prd_type)
)
WITH (
	OIDS=FALSE
) ;

COMMENT ON TABLE adl.mapp_comp_busi_dev_stats IS '移动驾驶舱-分公司展业统计汇总' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.comp_id IS '分公司ID' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.comp_shrt_nm IS '分公司简称' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.prd_type IS '期间类别' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.shr_cnt IS '分享次数' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.frwrd_cnt IS '被转发数' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.vstr_cnt IS '访客数量' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.brs_cnt IS '浏览数量' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.camp_invt_cust_cnt IS '线下活动邀请人数' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.camp_cust_arrv_cnt IS '线下到达到场人数' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.camp_cnt IS '线下活动数' ;
COMMENT ON COLUMN adl.mapp_comp_busi_dev_stats.rpt_dtm IS '报表时间' ;


