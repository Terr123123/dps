CREATE TABLE edw.dict_src_sys (
	sys_cd varchar(100) NULL, -- 门户源系统编码
	group_cd varchar(100) NULL, -- 门户分组编码
	group_nm varchar(100) NULL, -- 门户分组名称
	sys_alia_cd varchar(100) NULL, -- 源系统简称
	dict_key varchar(100) NULL, -- 门户数据字典键值
	dict_nm varchar(100) NULL, -- 门户数据字典名称
	edw_dict_group_cd varchar(100) NULL, -- 数据仓库数据字典分组
	edw_dict_key varchar(100) NULL, -- 数据仓库数据字典键值
	edw_dict_nm varchar(100) NULL -- 数据仓库数据字典名称
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX dict_src_sys_dic_key_dict_nm_idx ON edw.dict_src_sys USING btree (dict_key, dict_nm) ;
CREATE INDEX dict_src_sys_group_nm_group_cd_idx ON edw.dict_src_sys USING btree (group_nm, group_cd) ;
CREATE INDEX dict_src_sys_sys_alia_cd_idx ON edw.dict_src_sys USING btree (sys_alia_cd) ;

-- Column comments
COMMENT ON TABLE edw.dict_src_sys IS '源系统数据字典';
COMMENT ON COLUMN edw.dict_src_sys.sys_cd IS '门户源系统编码' ;
COMMENT ON COLUMN edw.dict_src_sys.group_cd IS '门户分组编码' ;
COMMENT ON COLUMN edw.dict_src_sys.group_nm IS '门户分组名称' ;
COMMENT ON COLUMN edw.dict_src_sys.sys_alia_cd IS '源系统简称' ;
COMMENT ON COLUMN edw.dict_src_sys.dict_key IS '门户数据字典键值' ;
COMMENT ON COLUMN edw.dict_src_sys.dict_nm IS '门户数据字典名称' ;
COMMENT ON COLUMN edw.dict_src_sys.edw_dict_group_cd IS '数据仓库数据字典分组' ;
COMMENT ON COLUMN edw.dict_src_sys.edw_dict_key IS '数据仓库数据字典键值' ;
COMMENT ON COLUMN edw.dict_src_sys.edw_dict_nm IS '数据仓库数据字典名称' ;