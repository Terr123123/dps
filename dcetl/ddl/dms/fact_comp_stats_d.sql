CREATE TABLE dms.fact_comp_stats_d (
	comp_id int NOT NULL, -- 分公司ID
	stats_dt date NOT NULL, -- 统计日期
	leads_cnt numeric NULL DEFAULT 0, -- 新增销售线索数
	shr_cnt numeric NULL DEFAULT 0, -- 分享次数
	brs_cnt numeric NULL DEFAULT 0, -- 浏览次数
	frwrd_cnt numeric NULL DEFAULT 0, -- 被转发数
	vstr_cnt numeric NULL DEFAULT 0, -- 访客数量
	camp_cnt int DEFAULT 0,  -- 线下活动数
	camp_invt_cust_cnt int8 NULL DEFAULT 0, -- 线下活动邀请人数
	camp_cust_arrv_cnt int8 NULL DEFAULT 0, -- 线下到达到场人数
	order_cnt int8 NULL DEFAULT 0, -- 预约客户数
	order_amnt numeric NULL DEFAULT 0, -- 预约金额
	pay_arrv_cust_cnt int8 NULL DEFAULT 0, -- 到账客户数
	pay_arrv_amnt numeric NULL DEFAULT 0, -- 到账金额
	CONSTRAINT fact_comp_stats_d_pk PRIMARY KEY (comp_id, stats_dt)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_comp_stats_d_stats_dt_idx ON dms.fact_comp_stats_d USING btree (stats_dt) ;

-- Column comments
COMMENT ON TABLE dms.fact_comp_stats_d IS '理财师日统计' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.comp_id IS '分公司ID' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.stats_dt IS '统计日期' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.leads_cnt IS '新增销售线索数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.shr_cnt IS '分享次数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.brs_cnt IS '浏览次数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.frwrd_cnt IS '被转发数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.vstr_cnt IS '访客数量' ; -- 去重了
COMMENT ON COLUMN dms.fact_comp_stats_d.camp_cnt IS '线下活动数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.camp_invt_cust_cnt IS '线下活动邀请人数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.camp_cust_arrv_cnt IS '线下到达到场人数' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.order_cnt IS '预约客户数' ; -- 去重了
COMMENT ON COLUMN dms.fact_comp_stats_d.order_amnt IS '预约金额' ;
COMMENT ON COLUMN dms.fact_comp_stats_d.pay_arrv_cust_cnt IS '到账客户数' ; -- 去重了
COMMENT ON COLUMN dms.fact_comp_stats_d.pay_arrv_amnt IS '到账金额' ;