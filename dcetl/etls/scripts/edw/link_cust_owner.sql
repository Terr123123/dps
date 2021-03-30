-- 不支持重跑历史，删除历史数据，为了安全只能删除当天数据，要删除历史数据请另外跑
delete from  edw.link_cust_owner where start_dt=current_date;
-- 删除历史表
drop table if exists tmp_link_cust_owner;
	-- 插入与拉链表匹配不上的数据，作为变化数据
create table tmp_link_cust_owner as
			select
			i.cust_no,
			coalesce(i.emp_cd,'未分配理财师') emp_cd,
			i.comp_id
			from edw.dim_cust i
			left join edw.link_cust_owner l
				 on i.cust_no=l.cust_no
				 and coalesce(i.emp_cd,'未分配理财师')=l.emp_cd
				 and i.comp_id=l.comp_id
				 and l.end_dt='9999-12-31'
			where l.cust_no is null; --  and i.emp_cd>'' ;

	-- 更新历史数据的有效截止日期
update edw.link_cust_owner link
				set end_dt='${batch_dt}'::date-1,upd_dtm=current_timestamp
	from(
			select
						l.cust_no,
						max(l.auto_id) auto_id
			from edw.link_cust_owner l
				inner join tmp_link_cust_owner c on c.cust_no=l.cust_no
			where l.end_dt>='${batch_dt}'::date-1
			group by l.cust_no
	) as up where up.auto_id=link.auto_id;
	-- 新数据插入 有效日期开始为当天，截止日期为9999-12-31
insert into edw.link_cust_owner(
	cust_no,
	emp_cd,
	comp_id,
	start_dt,
	end_dt)
select
	cust_no,
	emp_cd,
	comp_id,
	'${batch_dt}' start_dt, -- 有效开始日期为当天
	'9999-12-31'::date end_dt -- 有效截止日期为9999-12-31
from tmp_link_cust_owner;