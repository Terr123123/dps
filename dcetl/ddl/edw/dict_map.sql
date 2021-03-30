CREATE TABLE edw.dict_map (
        grp_key varchar(50) NOT NULL, -- 数据字典分类编号
        grp_nm varchar(100) not NULL, -- 数据字典分类名称
        grp_desc varchar(1000) NULL, -- 数据字典备注
        dict_type varchar(20) NULL, -- 数据字典类型
        dict_key varchar(50) NOT NULL default 'key', -- 数据字典K值
        dict_nm varchar(100) NOT NULL, -- 数据字典名称
        dict_rng_beg varchar(50)  NULL, -- 数据字典开始范围
        dict_rng_end varchar(50)  NULL, -- 数据字典结束范围
        dict_list varchar(100) NULL, -- 数据字典列举值
        src_sys_cd varchar(10) NULL DEFAULT 'sp'::character varying, -- 数据字典系统来源
        filed1 varchar(50) NULL, -- 备用字段1
        filed2 varchar(50) NULL, -- 备用字段2
        filed3 varchar(50) NULL, -- 备用字段3
        filed4 varchar(50) NULL, -- 备用字段4
        filed5 varchar(50) NULL, -- 备用字段5
        CONSTRAINT dict_map_pk PRIMARY KEY (grp_key, dict_key, dict_nm),
        constraint dict_map_dict_type_chk check (dict_type in('key','rng','list'))
)WITH (
    OIDS=FALSE
) ;
--  index
CREATE INDEX dict_map_src_sys_cd_idx ON edw.dict_map (src_sys_cd) ;
--   comments
COMMENT ON TABLE edw.dict_map IS 'DPS数据字典映射表' ;
COMMENT ON COLUMN edw.dict_map.grp_key IS '数据字典分类编号' ;
COMMENT ON COLUMN edw.dict_map.grp_nm IS '数据字典分类名称' ;
COMMENT ON COLUMN edw.dict_map.grp_desc IS '数据字典备注' ;
COMMENT ON COLUMN edw.dict_map.dict_type IS '数据字典类型' ;
-- dict_type 字典类型:key(kek value 方式) rng(按区间范围范围) list 列举所有
COMMENT ON COLUMN edw.dict_map.dict_key IS '数据字典K值' ;
COMMENT ON COLUMN edw.dict_map.dict_nm IS '数据字典名称' ;
COMMENT ON COLUMN edw.dict_map.dict_rng_beg IS '数据字典开始范围' ;
COMMENT ON COLUMN edw.dict_map.dict_rng_end IS '数据字典结束范围' ;
COMMENT ON COLUMN edw.dict_map.dict_list IS '数据字典列举值' ;
COMMENT ON COLUMN edw.dict_map.src_sys_cd IS '数据字典系统来源' ;
COMMENT ON COLUMN edw.dict_map.filed1 IS '备用字段1' ;
COMMENT ON COLUMN edw.dict_map.filed2 IS '备用字段2' ;
COMMENT ON COLUMN edw.dict_map.filed3 IS '备用字段3' ;
COMMENT ON COLUMN edw.dict_map.filed4 IS '备用字段4' ;
COMMENT ON COLUMN edw.dict_map.filed5 IS '备用字段5' ;
    -- src_sys_cd sp 表示人工特殊处理进去的 