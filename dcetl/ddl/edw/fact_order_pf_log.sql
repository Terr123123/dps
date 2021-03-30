CREATE TABLE edw.fact_order_pf_log (
	trans_cd varchar(20) NULL, -- 交易编号
	oper_type_cd varchar(10) NULL, -- 操作类型编号
	oper_type_nm varchar(100) NULL, -- 操作类型名称
  is_conf_oper varchar(1) NULL, -- 是否总审确认操作
	crt_dtm timestamp NULL -- 创建时间
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_order_pf_log_trans_cd_idx ON edw.fact_order_pf_log USING btree (trans_cd, oper_type_cd) ;
CREATE INDEX fact_order_pf_log_crt_dtm_idx ON edw.fact_order_pf_log USING btree (crt_dtm) ;

-- Column comments
COMMENT ON TABLE edw.fact_order_pf_log IS '私募交易操作日志' ;
COMMENT ON COLUMN edw.fact_order_pf_log.trans_cd IS '交易编号' ;
COMMENT ON COLUMN edw.fact_order_pf_log.oper_type_cd IS '操作类型编号' ;
COMMENT ON COLUMN edw.fact_order_pf_log.oper_type_nm IS '操作类型名称' ;
COMMENT ON COLUMN edw.fact_order_pf_log.is_conf_oper IS '是否总审确认操作' ;
COMMENT ON COLUMN edw.fact_order_pf_log.crt_dtm IS '创建时间' ;
