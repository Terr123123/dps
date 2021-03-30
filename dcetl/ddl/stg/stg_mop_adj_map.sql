CREATE TABLE stg.stg_mop_adj_map (
	order_cd varchar(50) NOT NULL, -- 订单号
	rule_id varchar(20) NOT NULL, -- 规则ID
	adj_std_kpi numeric(8,4) NULL, -- 调整KPI系数
	adj_std_amnt_by_ratio numeric(16,2) NULL, -- 按比率调整业绩金额
	adj_std_amnt numeric(16,2) NULL, -- 调整业绩金额
	rule_add_dt date NULL, -- 规则添加日期
	crt_dtm timestamp NULL DEFAULT now(), -- 创建时间
	CONSTRAINT stg_mop_adj_map_pk PRIMARY KEY (order_cd, rule_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX stg_mop_adj_map_adj_std_kpi_idx ON stg.stg_mop_adj_map USING btree (adj_std_kpi) ;
CREATE INDEX stg_mop_adj_map_rule_id_idx ON stg.stg_mop_adj_map USING btree (rule_id) ;

-- Column comments
COMMENT ON TABLE stg.stg_mop_adj_map IS '业绩调整映射表' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.order_cd IS '订单号' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.rule_id IS '规则ID' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.adj_std_kpi IS '调整KPI系数' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.adj_std_amnt_by_ratio IS '按比率调整业绩金额' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.adj_std_amnt IS '调整业绩金额' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.rule_add_dt IS '规则添加日期' ;
COMMENT ON COLUMN stg.stg_mop_adj_map.crt_dtm IS '创建时间' ;
