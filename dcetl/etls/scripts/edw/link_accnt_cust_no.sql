-- 删除历史数据，为了安全只能删除当天数据，要删除历史数据请另外跑
delete from  edw.link_accnt_cust_no where start_dt=current_date;
-- 删除历史表
drop table if exists tmp_link_accnt_cust_no;
	-- 插入与拉链表匹配不上的数据，作为变化数据
	create table tmp_link_accnt_cust_no as
			select
			i.accnt_cd,
			i.cust_no cust_no
			from edw.dim_accnt i
			left join edw.link_accnt_cust_no l
				 on i.accnt_cd=l.accnt_cd
				 and i.cust_no=l.cust_no
				 and l.end_dt>='${batch_dt}'::date-1
				where l.cust_no is null  and i.cust_no>0 ;

	-- 更新历史数据的有效截止日期
update edw.link_accnt_cust_no link
				set end_dt='${batch_dt}'::date-1,upd_dtm=current_timestamp
	from(
			select l.accnt_cd,max(l.auto_id) auto_id
			from edw.link_accnt_cust_no l
				inner join tmp_link_accnt_cust_no c on c.accnt_cd=l.accnt_cd
			where l.end_dt>='${batch_dt}'::date-1
			group by l.accnt_cd
	) as up where up.auto_id=link.auto_id;
	-- 新数据插入 有效日期开始为当天，截止日期为9999-12-31
insert into edw.link_accnt_cust_no(
					accnt_cd ,
					cust_no ,
					start_dt,
					end_dt)
select
					accnt_cd,
					cust_no ,
					'${batch_dt}' start_dt, -- 有效开始日期为当天
					'9999-12-31'::date end_dt -- 有效截止日期为9999-12-31
from tmp_link_accnt_cust_no;