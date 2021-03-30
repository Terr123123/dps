create table ods.elearning_exam_result (
    ceshiname varchar(200) null, -- 测试名称
    ceshiscore varchar(5) null, -- 测试分数
    ceshitime timestamp null, -- 测试时间
    itemcode varchar(30) null, -- 课程编号
    itemname varchar(200) null, -- 课程名称
    uname varchar(30) null, -- 员工名称
    uno varchar(30) null, -- 员工编号
    import_batch_nm varchar(50) null, -- 导入批次名称
    import_batch_dtm timestamp null -- 导入批次时间
)
with (
    oids=false
) ;
CREATE INDEX elearning_exam_result_ceshitime_idx ON ods.elearning_exam_result USING btree (ceshitime) ;
CREATE INDEX elearning_exam_result_itemcode_idx ON ods.elearning_exam_result USING btree (itemcode);

comment on table  ods.elearning_exam_result is 'elearnig测试分数';
comment on column ods.elearning_exam_result.ceshiname is '测试名称' ;
comment on column ods.elearning_exam_result.ceshiscore is '测试分数' ;
comment on column ods.elearning_exam_result.ceshitime is '测试时间' ;
comment on column ods.elearning_exam_result.itemcode is '课程编号' ;
comment on column ods.elearning_exam_result.itemname is '课程名称' ;
comment on column ods.elearning_exam_result.uname is '员工名称' ;
comment on column ods.elearning_exam_result.uno is '员工编号' ;
comment on column ods.elearning_exam_result.import_batch_nm is '导入批次名称' ;
comment on column ods.elearning_exam_result.import_batch_dtm is '导入批次时间' ;