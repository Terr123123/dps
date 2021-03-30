truncate table edw.dict_src_sys;
insert into edw.dict_src_sys
(
    sys_cd,
    group_cd,
    group_nm,
    sys_alia_cd,
    dict_key,
    dict_nm,
    edw_dict_group_cd,
    edw_dict_key,
    edw_dict_nm
)
SELECT
    DISTINCT
    s.system_code sys_cd,
    s.group_code group_cd,
    g.name group_nm,
    i.filed1 sys_alia_cd,
    s.value dic_key,
    trim(s.label) dic_nm,
    null dw_dic_group_cd,
    null dw_dic_key,
    null dw_dic_nm
FROM portal.hyb_value_set s
INNER JOIN portal.hyb_value_set_group g
ON s.system_code=g.system_code AND s.group_code=g.code
left JOIN edw.dict_map i ON s.system_code=i.dict_key and i.grp_key='src_sys_cd';