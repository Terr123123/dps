truncate table dms.fact_emp_exam_stats_by_exam;
insert into dms.fact_emp_exam_stats_by_exam(
            exam_cd,
            emp_join_cnt,
            avg_score,
            max_score,
            min_score,
            qual_score,
            qual_cnt,
            score90_to_100_cnt,
            score80_to_89_cnt,
            score70_to_79_cnt,
            score60_to_69_cnt,
            score_under60_cnt
        )
       select
            exam_cd,
            count( 1 ) emp_join_cnt, -- 参考人数
            round( avg( exam_score ), 2 ) avg_score,
            max( exam_score ) max_score,
            min( exam_score ) min_score,
            60 qual_score, -- 约定 满分均为100 及格分数60
            sum( case when exam_score >= 60 then 1 else 0 end ) qual_cnt,
            sum( case when exam_score >= 90 then 1 else 0 end ) score90_to_100_cnt,
            sum( case when exam_score between 80 and 89 then 1 else 0 end ) score80_to_89_cnt,
            sum( case when exam_score between 70 and 79 then 1 else 0 end ) score70_to_79_cnt,
            sum( case when exam_score between 60 and 69 then 1 else 0 end ) score60_to_69_cnt,
            sum( case when exam_score < 60 then 1 else 0 end ) score_under60_cnt
        from
            edw.fact_emp_exam_detl s where is_roadshow='Y'
        group by exam_cd;