--支持重跑历史，一旦重跑历史，则中间每一天都需要重新计算
--  备份需要重算的历史数据
drop table if exists stg.tmp_link_cust_prod_shr_hold_old;
create table  stg.tmp_link_cust_prod_shr_hold_old as
			select
			i.*
			from  edw.link_cust_prod_shr_hold i
		  where   start_dt>='${busi_dt}';
-- 删除需要重算的历史数据
delete from edw.link_cust_prod_shr_hold where start_dt>='${busi_dt}';
-- 对需要重算的历史数据的上个有效结束时间还原为9999-12-31,(还原历史)
update edw.link_cust_prod_shr_hold link
                set end_dt='9999-12-31',upd_dtm=current_timestamp
    from(
           select l.accnt_cd,max(l.auto_id) auto_id
			from edw.link_cust_prod_shr_hold l
			inner join stg.tmp_link_cust_prod_shr_hold_old c
               on c.accnt_cd=l.accnt_cd
               and c.prod_id=l.prod_id
               and c.prod_shr_type_id=l.prod_shr_type_id
			         and l.end_dt>='${busi_dt}'::date-1
			group by l.accnt_cd,l.prod_id,l.prod_shr_type_id
    ) as up
where  link.accnt_cd=up.accnt_cd and up.auto_id=link.auto_id ;
-- 开始重算拉链表
-- 插入与拉链表匹配不上的数据，作为变化数据
drop table if exists stg.tmp_link_cust_prod_shr_hold;
create table stg.tmp_link_cust_prod_shr_hold as
			select
			i.accnt_cd,
			i.prod_id,
			i.prod_shr_type_id,
			i.bal_amnt
			from (select
						accnt_cd,
						prod_id,
						prod_shr_type_id,
						round(bal_amnt,2)  bal_amnt
						from edw.his_fact_cust_prod_shr_hold where hold_dt='${busi_dt}') i
			left join edw.link_cust_prod_shr_hold l
				 on i.accnt_cd=l.accnt_cd
				 and i.prod_id=l.prod_id
				 and i.prod_shr_type_id=l.prod_shr_type_id
				 and i.bal_amnt=l.bal_amnt
				 and l.end_dt='9999-12-31'
				where l.accnt_cd is null  and i.accnt_cd>'';

-- 处理被删除的(当前比较不存在，但是拉链表存在的)
drop table if exists stg.tmp_link_cust_prod_shr_hold_del;
create table stg.tmp_link_cust_prod_shr_hold_del as
         select l.accnt_cd,l.prod_id,l.prod_shr_type_id,max(l.auto_id) auto_id
            from edw.link_cust_prod_shr_hold l
            left join  edw.his_fact_cust_prod_shr_hold c
                  on c.hold_dt='${busi_dt}'
                 and c.accnt_cd=l.accnt_cd
                 and c.prod_id=l.prod_id
                 and c.prod_shr_type_id=l.prod_shr_type_id
            where l.end_dt='9999-12-31' and c.accnt_cd is null and l.bal_amnt>0
            group by l.accnt_cd,l.prod_id,l.prod_shr_type_id;
-- 没有持有金额的当0处理,需要更新上个拉链数据截止日期
update edw.link_cust_prod_shr_hold link
                set end_dt='${busi_dt}'::date-1,upd_dtm=current_timestamp
    from stg.tmp_link_cust_prod_shr_hold_del as up
    where  link.accnt_cd=up.accnt_cd and up.auto_id=link.auto_id and link.end_dt='9999-12-31' and link.bal_amnt>0;

	-- 有新数据的，更新历史拉链的有效截止日期
update edw.link_cust_prod_shr_hold link
				set end_dt='${busi_dt}'::date-1,upd_dtm=current_timestamp
	from(
			select l.accnt_cd,max(l.auto_id) auto_id
			from edw.link_cust_prod_shr_hold l
			inner join stg.tmp_link_cust_prod_shr_hold c
               on c.accnt_cd=l.accnt_cd
                 and c.prod_id=l.prod_id
                 and c.prod_shr_type_id=l.prod_shr_type_id
			where l.end_dt='9999-12-31'
			group by l.accnt_cd,l.prod_id,l.prod_shr_type_id
	) as up where  link.accnt_cd=up.accnt_cd and up.auto_id=link.auto_id and link.end_dt='9999-12-31';
	-- 新数据插入 有效日期开始为当天，截止日期为9999-12-31
insert into edw.link_cust_prod_shr_hold(
					accnt_cd,
          prod_id,
          prod_shr_type_id,
          bal_amnt,
          start_dt,
          end_dt)
select
			i.accnt_cd,
			i.prod_id,
			i.prod_shr_type_id,
			i.bal_amnt,
			'${busi_dt}' start_dt, -- 有效开始日期为当天
			'9999-12-31'::date end_dt -- 有效截止日期为9999-12-31
from stg.tmp_link_cust_prod_shr_hold i;
-- 持仓变为0的是否加入数据
