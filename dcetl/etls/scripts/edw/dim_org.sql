truncate table edw.dim_org;
insert into edw.dim_org(
    org_id	,--	'机构ID'
    src_org_cd	,--	'源系统机构编号'
    org_nm	,--	'机构名称'
    is_comp	,--	'是否分公司'
    is_actv	,--	'是否有效'
    org_shrt_nm	,--	'机构简称'
    org_type	,--	'机构类型'
    org_mgr_cd	,--	'机构管理人工号'
    city_org_id	,--	'城市机构ID'
    city_org_nm	,--	'城市机构名称'
    city_org_mgr_cd	,--	'城市机构管理人工号'
    area_org_id	,--	'区域机构ID'
    area_org_nm	,--	'区域机构名称'
    area_org_mgr_cd	,--	'区域机构管理人工号'
    vp_cd	,--	'VP工号'
    vp_nm	,--	'VP名称'
    vp_sort_idx	,--	'VP排序'
    show_vp_nm	,--	'VP显示名称'
    ech_cd	,--	'梯队编号'
    ech_nm	,--	'梯队名称'
    sort_idx	,--	'展示排序'
    vp_is_agent	,--	'VP是否代理'
    open_dt	,--	'成立日期'
    close_dt	,--	'关闭日期'
    city_org_show_nm	,--	'城市显示名称'
    work_capc	--	'职场容量'
)
select
 s.org_id,
 s.src_org_cd, 
 s.org_nm,
 s.is_comp,
 s.is_valid,
 case when s.org_type='company' then  trim('分公司' from s.org_nm) else s.org_nm end     as org_shrt_nm, -- 名称简称
 case when s.org_type='company' then  'comp' else s.org_type end     as org_type,
 s.org_mgr_cd org_mgr_cd,
 s2.org_id city_org_id,
 s2.org_nm city_org_nm,
 s2.org_mgr_cd city_org_mgr_cd,
 s1.org_id area_org_id,
 s1.org_nm area_org_nm,
 s1.org_mgr_cd area_org_mgr_cd,
 case when length(i.emp_code_vp)>5 then trim(i.emp_code_vp) else trim(i.emp_code_avp) end vp_cd,
 case when e.emp_nm is not null and i.is_agent='1' then e.emp_nm||'[代]'
     when e.emp_nm is not null and i.is_agent<>'1' then e.emp_nm
     else '未分vp分公司' end vp_nm,  -- vp名称
 coalesce(c.sort_index,99) vp_sort_idx,
 case when e.emp_nm is not null and i.is_agent='1' then e.emp_nm||'[二]'
     when e.emp_nm is not null and i.is_agent<>'1' then e.emp_nm
     else '未分vp分公司' end show_vp_nm,
 coalesce(i.echelon,'x')                             as ech_cd, -- 梯队名称
 coalesce(n.echelon_name,'未分配梯队')                as ech_nm, -- 梯队名称
 coalesce(i.sort_index,99999)     as sort_idx,
 i.is_agent,
 i.check_begin_date  as open_dt,
 i.check_end_date    as close_dt,
 coalesce(e2.emp_nm||'_区域',
                    (case when e.emp_nm is not null and i.is_agent='1' then e.emp_nm||'[二]'
                     when e.emp_nm is not null and i.is_agent<>'1' then e.emp_nm
                     else '未分vp分公司' end)||'未分_区域'
                ) as city_comp_show_nm,    -- 报表区域展示名称
    i.work_capc         
 from stg.stg_dim_org s
 left join stg.stg_dim_org s2 on s.lvl2=s2.org_id  -- 城市等级
 left join stg.stg_dim_org s1 on s.lvl1=s1.org_id   -- 区域等级
 left join web.comp_info i on s.org_id=i.comp_id
 left join edw.dim_emp e on case when length(i.emp_code_vp)>5 then trim(i.emp_code_vp) else trim(i.emp_code_avp) end=e.emp_cd
 left join edw.dim_emp e2 on s2.org_mgr_cd=e.emp_cd
 left join web.vp_sort c on case when length(i.emp_code_vp)>5 then trim(i.emp_code_vp) else trim(i.emp_code_avp) end=c.emp_code
 left join dd.echelon_info n on i.echelon=n.echelon_id