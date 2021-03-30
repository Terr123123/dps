CREATE TABLE edw.fact_prod_net_val (
        shr_type_id  varchar(20)  NULL, -- 产品份额类别id
        prod_id varchar(50) NULL, -- 产品ID
        net_val_dt date NULL, -- 净值日期
        crt_dtm timestamp NULL, -- 创建时间
        net_val numeric(7,4) NULL, -- 当日净值
        accum_net_val numeric(7,4) NULL, -- 累计净值
        seven_day_aror numeric(8,5) NULL, -- 七日年化收益率
        ten_thsd_acrl numeric(8,5) NULL, -- 万份收益
--         prod_type_cd varchar(50) NULL, -- 产品类型编号
        sys_sys_cd varchar(50) NULL, -- 源系统编号
       CONSTRAINT fact_prod_net_val_pk PRIMARY KEY (prod_id, net_val_dt, shr_type_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_prod_net_val_net_val_dt_idx ON edw.fact_prod_net_val USING btree (net_val_dt) ;
-- Column comments
COMMENT ON TABLE edw.fact_prod_net_val IS '产品净值事实表';
COMMENT ON COLUMN edw.fact_prod_net_val.shr_type_id IS '产品份额类别id' ;
COMMENT ON COLUMN edw.fact_prod_net_val.prod_id IS '产品ID' ;
COMMENT ON COLUMN edw.fact_prod_net_val.net_val_dt IS '净值日期' ;
COMMENT ON COLUMN edw.fact_prod_net_val.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.fact_prod_net_val.net_val IS '当日净值' ;
COMMENT ON COLUMN edw.fact_prod_net_val.accum_net_val IS '累计净值' ;
COMMENT ON COLUMN edw.fact_prod_net_val.seven_day_aror IS '七日年化收益率' ;
COMMENT ON COLUMN edw.fact_prod_net_val.ten_thsd_acrl IS '万份收益' ;
COMMENT ON COLUMN edw.fact_prod_net_val.sys_sys_cd IS '源系统编号' ;