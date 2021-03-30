delete  from edw.fact_busi_dev_emp_acc_used where check_ym= substring('${busi_dt}',0,8);
insert into edw.fact_busi_dev_emp_acc_used (
	emp_cd , -- 员工号
	emp_nm  , -- 员工名称
	hr_job_nm  , -- 人事岗位名称
	join_dt  , -- 入职日期
	start_used_dt  , -- 开始使用日期
	comp_id , -- 分公司ID
	emp_stat , -- 在职状态
	used_flag  , -- 使用标识
	if_check_kpi,   -- 是否考核KPI
     check_ym
)
select e.emp_cd,e.emp_nm,e.hr_job_nm,e.join_dt,t.start_used_dt,e.comp_id,e.emp_stat,
   case when t.start_used_dt is null then 0 else 1 end used_flag,
   case when e.emp_stat<>'离职' and e.join_dt<=to_date(substring('${busi_dt}',0,8)||'-15','YYYY-MM-DD')
              then 1 else 0 end if_check_kpi,substring('${busi_dt}',0,8) check_ym
   -- c.comp_nm,c.show_nm vp_show_nm,c.comp_mgr_nm
   from dw.dim_emp e left join stg.stg_busi_dev_emp_used t
   on e.emp_cd=t.emp_cd
   -- left join dw.dim_comp c on e.comp_id=c.comp_id
   where e.emp_cat='前台' and (e.left_dt is null or  e.left_dt>='2020-09-01');

