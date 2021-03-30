CREATE TABLE edw.dim_prod_open (
    prod_open_id varchar(20) NULL, -- 开放标识
    prod_id varchar(50) NULL, -- 产品编号
    open_rank_nm int4 NULL, -- 开放期次
    open_beg_dt date NULL, -- 开放开始日期
    open_end_dt date NULL, -- 开放截止日期
    ordr_beg_dt date NULL, -- 预约发起开始日期
    ordr_end_dt date NULL, -- 预约发起结束日期
    open_type varchar(50), -- 开放类型（募集期，开放期）
    CONSTRAINT dim_prod_open_pk PRIMARY KEY (prod_open_id)
--     sg_stat varchar(2) NULL, -- 申购状态
--     sh_stat varchar(2) NULL, -- 赎回状态
--     zr_stat varchar(2) NULL -- 转让状态
)
WITH (
    OIDS=FALSE
) ;
CREATE UNIQUE INDEX dim_prod_open_prod_id_open_dt_idx ON edw.dim_prod_open USING btree (prod_id, open_beg_dt) ;
-- Column comments
COMMENT ON TABLE edw.dim_prod_open IS '产品开放期维度表';
COMMENT ON COLUMN edw.dim_prod_open.prod_open_id IS '开放标识' ;
COMMENT ON COLUMN edw.dim_prod_open.prod_id IS '产品编号' ;
COMMENT ON COLUMN edw.dim_prod_open.open_rank_nm IS '开放期次' ;
COMMENT ON COLUMN edw.dim_prod_open.open_beg_dt IS '开放开始日期' ;
COMMENT ON COLUMN edw.dim_prod_open.open_end_dt IS '开放截止日期' ;
COMMENT ON COLUMN edw.dim_prod_open.ordr_beg_dt IS '预约发起开始日期' ;
COMMENT ON COLUMN edw.dim_prod_open.ordr_end_dt IS '预约发起结束日期' ;
-- COMMENT ON COLUMN edw.dim_prod_open.sg_stat IS '申购状态' ;
-- COMMENT ON COLUMN edw.dim_prod_open.sh_stat IS '赎回状态' ;
-- COMMENT ON COLUMN edw.dim_prod_open.zr_stat IS '转让状态' ;