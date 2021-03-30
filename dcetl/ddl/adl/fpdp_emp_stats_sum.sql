CREATE TABLE adl.fpdp_emp_stats_sum (
	emp_cd varchar(20) NOT NULL, -- 理财师编号
	prd_type varchar(20) NOT NULL, -- 期间类别
	leads_cnt numeric NULL DEFAULT 0, -- 新增销售线索数
	shr_cnt numeric NULL DEFAULT 0, -- 分享次数
	brs_cnt numeric NULL DEFAULT 0, -- 浏览次数
	frwrd_cnt numeric NULL DEFAULT 0, -- 被转发数
	vstr_cnt numeric NULL DEFAULT 0, -- 访客数量
	camp_invt_cust_cnt int8 NULL DEFAULT 0, -- 线下活动邀请人数
	camp_cust_arrv_cnt int8 NULL DEFAULT 0, -- 线下到达到场人数
	order_cnt int8 NULL DEFAULT 0, -- 预约客户数
	order_amnt numeric NULL DEFAULT 0, -- 预约金额
	pay_arrv_cust_cnt int8 NULL DEFAULT 0, -- 到账客户数
	pay_arrv_amnt numeric NULL DEFAULT 0, -- 到账金额
	rpt_dtm timestamp DEFAULT now() , -- 报表时间
	CONSTRAINT fpdp_emp_stats_sum_pk PRIMARY KEY (emp_cd, prd_type)
)
WITH (
	OIDS=FALSE
) ;

COMMENT ON TABLE adl.fpdp_emp_stats_sum IS '个人数据平台-理财师展业统计汇总' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.emp_cd IS '理财师编号' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.prd_type IS '期间类别' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.leads_cnt IS '新增销售线索数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.shr_cnt IS '分享次数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.brs_cnt IS '浏览次数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.frwrd_cnt IS '被转发数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.vstr_cnt IS '访客数量' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.camp_invt_cust_cnt IS '线下活动邀请人数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.camp_cust_arrv_cnt IS '线下到达到场人数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.order_cnt IS '预约客户数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.order_amnt IS '预约金额' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.pay_arrv_cust_cnt IS '到账客户数' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.pay_arrv_amnt IS '到账金额' ;
COMMENT ON COLUMN adl.fpdp_emp_stats_sum.rpt_dtm IS '报表时间' ;


