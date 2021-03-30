CREATE TABLE edw.dim_org (
	org_id int4 NOT NULL, -- 机构ID
	src_org_cd varchar(20) NULL, -- 源系统机构编号
	org_nm varchar(100) NOT NULL, -- 机构名称
	is_comp varchar(1) NULL, -- 是否分公司
	is_actv varchar(1) NULL, -- 是否有效
	org_shrt_nm varchar(50) NULL, -- 机构简称
	org_type varchar(20) NULL, -- 机构类型
	org_mgr_cd varchar(20) NULL, -- 机构管理人工号
	city_org_id int4 NULL, -- 城市机构ID
	city_org_nm varchar(100) NULL, -- 城市机构名称
	city_org_mgr_cd varchar(20) NULL, -- 城市机构管理人工号
	area_org_id int4 NULL, -- 区域机构ID
	area_org_nm varchar(100) NULL, -- 区域机构名称
	area_org_mgr_cd varchar(20) NULL, -- 区域机构管理人工号
	vp_cd varchar(20) NULL, -- VP工号
	vp_nm varchar(50) NULL, -- VP名称
	vp_sort_idx int4 NULL, -- VP排序
	show_vp_nm varchar(50) NULL, -- VP显示名称
	ech_cd varchar(50) NULL, -- 梯队编号
	ech_nm varchar(50) NULL, -- 梯队名称
	sort_idx int4 NULL, -- 展示排序
	vp_is_agent varchar(1) NULL, -- VP是否代理
	open_dt date NULL, -- 成立日期
	close_dt date NULL, -- 关闭日期
	city_org_show_nm varchar(50) NULL, -- 城市显示名称
	work_capc int4 NULL--, -- 职场容量
	--CONSTRAINT dim_org_pk PRIMARY KEY (org_id,org_nm)
)
WITH (
	OIDS=FALSE
)
  DISTRIBUTED RANDOMLY
;

-- Column comments
COMMENT ON TABLE edw.dim_org IS '机构维度信息' ;
COMMENT ON COLUMN edw.dim_org.org_id IS '机构ID' ;
COMMENT ON COLUMN edw.dim_org.src_org_cd IS '源系统机构编号' ;
COMMENT ON COLUMN edw.dim_org.org_nm IS '机构名称' ;
COMMENT ON COLUMN edw.dim_org.is_comp IS '是否分公司' ;
COMMENT ON COLUMN edw.dim_org.is_actv IS '是否有效' ;
COMMENT ON COLUMN edw.dim_org.org_shrt_nm IS '机构简称' ;
COMMENT ON COLUMN edw.dim_org.org_type IS '机构类型' ;
COMMENT ON COLUMN edw.dim_org.org_mgr_cd IS '机构管理人工号' ;
COMMENT ON COLUMN edw.dim_org.city_org_id IS '城市机构ID' ;
COMMENT ON COLUMN edw.dim_org.city_org_nm IS '城市机构名称' ;
COMMENT ON COLUMN edw.dim_org.city_org_mgr_cd IS '城市机构管理人工号' ;
COMMENT ON COLUMN edw.dim_org.area_org_id IS '区域机构ID' ;
COMMENT ON COLUMN edw.dim_org.area_org_nm IS '区域机构名称' ;
COMMENT ON COLUMN edw.dim_org.area_org_mgr_cd IS '区域机构管理人工号' ;
COMMENT ON COLUMN edw.dim_org.vp_cd IS 'VP工号' ;
COMMENT ON COLUMN edw.dim_org.vp_nm IS 'VP名称' ;
COMMENT ON COLUMN edw.dim_org.vp_sort_idx IS 'VP排序' ;
COMMENT ON COLUMN edw.dim_org.show_vp_nm IS 'VP显示名称' ;
COMMENT ON COLUMN edw.dim_org.ech_cd IS '梯队编号' ;
COMMENT ON COLUMN edw.dim_org.ech_nm IS '梯队名称' ;
COMMENT ON COLUMN edw.dim_org.sort_idx IS '展示排序' ;
COMMENT ON COLUMN edw.dim_org.vp_is_agent IS 'VP是否代理' ;
COMMENT ON COLUMN edw.dim_org.open_dt IS '成立日期' ;
COMMENT ON COLUMN edw.dim_org.close_dt IS '关闭日期' ;
COMMENT ON COLUMN edw.dim_org.city_org_show_nm IS '城市显示名称' ;
COMMENT ON COLUMN edw.dim_org.work_capc IS '职场容量' ;