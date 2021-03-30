SET statement_mem='2GB';
insert into stg.stg_busi_dev_emp_used(emp_cd,start_used_dt)
select t.owner_id emp_cd  ,
min(time_created)::date start_used_dt
   from ods.busi_dev_activity t
   left join stg.stg_busi_dev_emp_used s on t.owner_id=s.emp_cd
   where t.owner_id>'' and t.time_created>=current_date-${p_days} and s.start_used_dt is null
 group by   t.owner_id ;
 show statement_mem;
