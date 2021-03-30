CREATE TABLE stg.stg_dim_org (
	org_id int4 NOT NULL, -- 机构ID
	src_org_cd varchar(20) NULL, -- 源系统机构ID
	org_nm varchar(100) NULL, -- 机构名称
	is_valid varchar(1) NULL, -- 是否有效
	org_mgr_cd varchar(20) NULL, -- 机构管理人工号
	parent_org_id int4 NULL, -- 上级机构ID
	parent_org_nm varchar(100) NULL, -- 上级机构名称
	parent_org_mgr_cd varchar(20) NULL, -- 上级机构管理人工号
	org_type varchar(20) NULL, -- 机构类别
	is_comp varchar(1) NULL, -- 是否分公司
	org_seq varchar(100) NULL, -- 机构序列号
	org_lvl int4 NULL, -- 机构等级
	comp_list varchar(100) NULL, -- 计算后机构等级列表
	lvl_list varchar(50) NULL, -- 机构等级列表
	lvl0 int4 NOT NULL, -- 一级机构ID
	lvl1 int4 NOT NULL, -- 二级机构ID
	lvl2 int4 NOT NULL, -- 三级机构ID
	lvl3 int4 NOT NULL, -- 四级机构ID
	CONSTRAINT stg_dim_org_pk PRIMARY KEY (org_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX stg_dim_org_id_idx ON stg.stg_dim_org USING btree (org_id) ;

COMMENT ON TABLE stg.stg_dim_org IS '机构计算中间表' ;
COMMENT ON COLUMN stg.stg_dim_org.org_id IS '机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.src_org_cd IS '源系统机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.org_nm IS '机构名称' ;
COMMENT ON COLUMN stg.stg_dim_org.is_valid IS '是否有效' ;
COMMENT ON COLUMN stg.stg_dim_org.org_mgr_cd IS '机构管理人工号' ;
COMMENT ON COLUMN stg.stg_dim_org.parent_org_id IS '上级机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.parent_org_nm IS '上级机构名称' ;
COMMENT ON COLUMN stg.stg_dim_org.parent_org_mgr_cd IS '上级机构管理人工号' ;
COMMENT ON COLUMN stg.stg_dim_org.org_type IS '机构类别' ;
COMMENT ON COLUMN stg.stg_dim_org.is_comp IS '是否分公司' ;
COMMENT ON COLUMN stg.stg_dim_org.org_seq IS '机构序列号' ;
COMMENT ON COLUMN stg.stg_dim_org.org_lvl IS '机构等级' ;
COMMENT ON COLUMN stg.stg_dim_org.comp_list IS '计算后机构等级列表' ;
COMMENT ON COLUMN stg.stg_dim_org.lvl_list IS '机构等级列表' ;
COMMENT ON COLUMN stg.stg_dim_org.lvl0 IS '一级机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.lvl1 IS '二级机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.lvl2 IS '三级机构ID' ;
COMMENT ON COLUMN stg.stg_dim_org.lvl3 IS '四级机构ID' ;