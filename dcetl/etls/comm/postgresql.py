# cython: language_level=3
import re
from etls.comm.loggers import get_logger
import pandas as pd

logger = get_logger()


def update_table_info(conn, crt_tb_sql, schm_tb_nm, test_tb_nm, test_schm, trgt_tb, trgt_schm):
    # 不重新建表 重新建临时表
    drop_sql = "DROP TABLE IF EXISTS {schm}.{tb};".format(schm=test_schm, tb=trgt_tb)
    crt_tb_sql = re.sub(schm_tb_nm, test_tb_nm, crt_tb_sql, flags=re.I)
    ddl = drop_sql + "\n" + crt_tb_sql
    conn.exec(ddl, if_print_sql=False)
    # 字段变化校验(1.备注修改  2.类型修改  3.是否为空修改  4.新增字段)
    logger.debug('%s 字段变化校验' % schm_tb_nm)
    check_sql = """
    WITH tp_test AS ( -- 临时表
        SELECT
            B.relname tb_nm,
            A.attname col_name,
            COALESCE(col_description(A.attrelid,A.attnum),'无备注') col_cmnt,
            CASE WHEN D.udt_name LIKE '%char' THEN D.udt_name||'('||COALESCE(D.character_maximum_length,'10')||')'
                 WHEN D.udt_name='numeric' OR D.udt_name LIKE 'float%' OR D.udt_name LIKE 'double%'
                 THEN 'numeric('||COALESCE(D.numeric_precision,'16')||','||COALESCE(D.numeric_scale,'4')||')'
                 ELSE D.udt_name END col,
            CASE WHEN D.is_nullable='YES' THEN 'null' ELSE 'not null' END is_null
        FROM pg_attribute A
        INNER JOIN pg_catalog.pg_class B
            ON B.oid=A.attrelid
            AND A.attnum>0 AND B.relname='{tb_nm}'
        INNER JOIN pg_catalog.pg_namespace C
            ON C.oid=B.relnamespace
            AND C.nspname='{test_schm}'
        INNER JOIN information_schema.columns D
            ON D.table_name=B.relname
            AND D.column_name=A.attname
            AND D.table_schema=C.nspname
    ),
    tp_dev AS ( -- 原表
        SELECT
            B.relname tb_nm,
            A.attname col_name,
            COALESCE(col_description(A.attrelid,A.attnum),'无备注') col_cmnt,
            CASE WHEN D.udt_name LIKE '%char' THEN D.udt_name||'('||COALESCE(D.character_maximum_length,'10')||')'
                 WHEN D.udt_name='numeric' OR D.udt_name LIKE 'float%' OR D.udt_name LIKE 'double%'
                 THEN 'numeric('||COALESCE(D.numeric_precision,'16')||','||COALESCE(D.numeric_scale,'4')||')'
                 ELSE D.udt_name END col,
            CASE WHEN D.is_nullable='YES' THEN 'null' ELSE 'not null' END is_null
        FROM pg_attribute A
        INNER JOIN pg_catalog.pg_class B
            ON B.oid=A.attrelid
            AND A.attnum>0 AND B.relname='{tb_nm}'
        INNER JOIN pg_catalog.pg_namespace C
            ON C.oid=B.relnamespace
            AND C.nspname='{trgt_schm}'
        INNER JOIN information_schema.columns D
            ON D.table_name=B.relname
            AND D.column_name=A.attname
            AND D.table_schema=C.nspname
    )
    SELECT N.col_name, N.col_cmnt, N.col col_type, N.is_null, '备注修改' chg_type
    FROM tp_test N
    LEFT JOIN tp_dev O ON N.col_name=O.col_name
    WHERE N.col_cmnt<>O.col_cmnt
    UNION ALL
    SELECT N.col_name, N.col_cmnt, N.col col_type, N.is_null, '类型修改' chg_type
    FROM tp_test N
    LEFT JOIN tp_dev O ON N.col_name=O.col_name
    WHERE N.col<>O.col
    UNION ALL
    SELECT N.col_name, N.col_cmnt, N.col col_type, N.is_null, '是否为空修改' chg_type
    FROM tp_test N
    LEFT JOIN tp_dev O ON N.col_name=O.col_name
    WHERE N.is_null<>O.is_null
    UNION ALL
    SELECT N.col_name, N.col_cmnt, N.col col_type, N.is_null, '新增字段' chg_type
    FROM tp_test N
    LEFT JOIN tp_dev O ON N.col_name=O.col_name
    WHERE O.col_name IS NULL;
    """
    check_df = conn.select(check_sql.format(tb_nm=trgt_tb, test_schm=test_schm, trgt_schm=trgt_schm))
    if check_df.shape[0] > 0:
        # logger.info(check_df)
        # cur = conn.cursor()
        for i in check_df.index:
            col_name = check_df.loc[i, 'col_name']
            col_cmnt = check_df.loc[i, 'col_cmnt']
            col_type = check_df.loc[i, 'col_type']
            is_null = check_df.loc[i, 'is_null']
            chg_type = check_df.loc[i, 'chg_type']
            if chg_type == '类型修改':
                modify = "ALTER TABLE {trgr_tb} ALTER COLUMN {col_name} TYPE {col_type} USING {col_name}::{col_type};"
                modify = modify.format(trgr_tb=schm_tb_nm, col_name=col_name, col_type=col_type)
                logger.debug('类型修改:{0}'.format(modify))
                # cur.execute(modify)
                # conn.commit()
                conn.exec(modify, if_print_sql=False)
            elif chg_type == '备注修改':
                modify = "COMMENT ON COLUMN {trgr_tb}.{col_name} IS '{col_cmnt}';"
                modify = modify.format(trgr_tb=schm_tb_nm, col_name=col_name, col_cmnt=col_cmnt)
                logger.debug('备注修改:{0}'.format(modify))
                # cur.execute(modify)
                # conn.commit()
                conn.exec(modify, if_print_sql=False)
            elif chg_type == '是否为空修改':
                if is_null == 'null':
                    modify = "ALTER TABLE {trgr_tb} ALTER COLUMN {col_name} DROP NOT NULL;"
                else:
                    modify = "ALTER TABLE {trgr_tb} ALTER COLUMN {col_name} SET NOT NULL;"
                modify = modify.format(trgr_tb=schm_tb_nm, col_name=col_name)
                logger.debug('是否为空修改:{0}'.format(modify))
                conn.exec(modify, if_print_sql=False)
            elif chg_type == '新增字段':
                modify = "ALTER TABLE  {trgr_tb}  ADD {col_name} {col_type} {is_null};"
                modify = modify.format(trgr_tb=schm_tb_nm, col_name=col_name, col_cmnt=col_cmnt,
                                       col_type=col_type,
                                       is_null=is_null)
                logger.debug('新增字段:{0}'.format(modify))
                # cur.execute(modify)
                conn.exec(modify, if_print_sql=False)
                modify = "COMMENT ON COLUMN {trgr_tb}.{col_name} IS '{col_cmnt}';"
                modify = modify.format(trgr_tb=schm_tb_nm, col_name=col_name, col_cmnt=col_cmnt)
                # cur.execute(modify)
                # conn.commit()
                conn.exec(modify, if_print_sql=False)
    # 主键校验
    old_pk_df = get_table_pk(conn, trgt_tb, trgt_schm)
    new_pk_df = get_table_pk(conn, test_tb_nm, test_schm)
    if old_pk_df.shape[0] > 0 and new_pk_df.shape[0] > 0:
        if old_pk_df.loc[0, 'pk_nm'] != new_pk_df.loc[0, 'pk_nm'] \
                or old_pk_df.loc[0, 'pk_cols'] != new_pk_df.loc[0, 'pk_cols']:
            # 修改主键 先删除再添加
            sql = """ ALTER TABLE {0} DROP CONSTRAINT {1} ;
                      ALTER TABLE {2}  ADD CONSTRAINT {3} PRIMARY KEY ({4});"""
            sql = sql.format(schm_tb_nm, old_pk_df.loc[0, 'pk_nm'], schm_tb_nm,
                             new_pk_df.loc[0, 'pk_nm'], new_pk_df.loc[0, 'pk_cols'])
            conn.exec(sql, if_print_sql=False)
    elif old_pk_df.shape[0] > 0 and new_pk_df.shape[0] < 1:
        # 删除主键
        sql = """ ALTER TABLE {0} DROP CONSTRAINT {1} ;"""
        sql = sql.format(schm_tb_nm, old_pk_df.loc[0, 'pk_nm'])
        conn.exec(sql, if_print_sql=False)
    elif old_pk_df.shape[0] < 1 and new_pk_df.shape[0] > 0:
        # 添加主键
        sql = """ ALTER TABLE {0}  ADD CONSTRAINT {1} PRIMARY KEY ({2});"""
        sql = sql.format(schm_tb_nm, new_pk_df.loc[0, 'pk_nm'], new_pk_df.loc[0, 'pk_cols'])
        conn.exec(sql, if_print_sql=False)
    # 索引表名一致性校验
    check_sql = """
    WITH tp_test AS (
        SELECT
            a.relname tb_nm,
            COALESCE(t.description,'无表名') tb_nm_cn,
            c.relname idx_nm,
            REPLACE(REPLACE(pg_get_indexdef(b.indexrelid),'{test_schm}.','{trgt_schm}.'),'public.','') proc
        FROM pg_class a
        INNER JOIN pg_index b                   ON a.oid=b.indrelid AND a.relname='{tb_nm}'
        INNER JOIN pg_catalog.pg_namespace n    ON n.oid=a.relnamespace AND n.nspname='{test_schm}'
        INNER JOIN pg_class c                   ON c.oid=b.indexrelid
        LEFT JOIN pg_description t              ON t.objsubid=0 AND a.oid=t.objoid
    ),
    tp_dev AS (
        SELECT
            a.relname tb_nm,
            COALESCE(t.description,'无表名') tb_nm_cn,
            c.relname idx_nm,
            pg_get_indexdef(b.indexrelid) proc
        FROM pg_class a
        INNER JOIN pg_index b                   ON a.oid=b.indrelid AND a.relname='{tb_nm}'
        INNER JOIN pg_catalog.pg_namespace n    ON n.oid=a.relnamespace AND n.nspname='{trgt_schm}'
        INNER JOIN pg_class c                   ON c.oid=b.indexrelid
        LEFT JOIN pg_description t              ON t.objsubid=0 AND a.oid=t.objoid
    )
    SELECT t.idx_nm,t.proc,'新增索引' chg_type FROM tp_test t
    LEFT JOIN tp_dev d ON t.idx_nm=d.idx_nm
    WHERE d.idx_nm IS NULL
    UNION ALL
    SELECT t.idx_nm,t.proc,'修改索引' chg_type FROM tp_test t
    LEFT JOIN tp_dev d ON t.idx_nm=d.idx_nm
    WHERE d.proc<>t.proc
    UNION ALL
    SELECT d.idx_nm,'' proc,'删除索引' chg_type FROM tp_test t
    RIGHT JOIN tp_dev d ON t.idx_nm=d.idx_nm
    WHERE  t.proc IS NULL
    UNION ALL
    SELECT DISTINCT t.tb_nm,t.tb_nm_cn,'修改表名' chg_type FROM tp_test t
    INNER JOIN tp_dev d ON t.idx_nm=d.idx_nm AND t.tb_nm_cn<>'无表名'
    WHERE d.tb_nm_cn<>t.tb_nm_cn;
    """
    logger.debug('%s 索引以及表名校验' % schm_tb_nm)
    check_df = conn.select(check_sql.format(tb_nm=trgt_tb, test_schm=test_schm, trgt_schm=trgt_schm))
    if check_df.shape[0] > 0:
        for i in check_df.index:
            chg_type = check_df.loc[i, 'chg_type']
            if chg_type in ['新增索引', '修改索引', '删除索引']:
                if chg_type == '删除索引':  # 可能存在约束删除
                    modify_sql = """
                    ALTER TABLE {trgt_schm}.{tb_nm} DROP CONSTRAINT IF EXISTS {idx_nm};
                    DROP INDEX IF EXISTS {trgt_schm}.{idx_nm};
                    {proc}
                    """
                else:
                    modify_sql = """DROP INDEX IF EXISTS {trgt_schm}.{idx_nm};{proc}"""

                modify_sql = modify_sql.format(trgt_schm=trgt_schm, tb_nm=trgt_tb,
                                               idx_nm=check_df.loc[i, 'idx_nm'],
                                               proc=check_df.loc[i, 'proc'])
                logger.debug('{0}'.format(chg_type))
                conn.exec(modify_sql, if_print_sql=False)
            if chg_type == '修改表名':
                modify_sql = """COMMENT ON TABLE {trgt_schm}.{idx_nm} IS '{proc}';"""
                modify_sql = modify_sql.format(trgt_schm=trgt_schm, idx_nm=check_df.loc[i, 'idx_nm'],
                                               proc=check_df.loc[i, 'proc'])
                # cur.execute(modify_sql)
                conn.exec(modify_sql, if_print_sql=False)
                logger.debug('修改表名:{0}'.format(modify_sql))
    logger.debug('%s 校验默认值变化' % schm_tb_nm)
    check_dufault_sql = """
    with tp_test AS (
        select 
        column_name col_nm,
        REPLACE(REPLACE(column_default,'{test_schm}.','{trgt_schm}.'),'public.','')  column_default_new 
        from information_schema.columns 
        where  table_schema='{test_schm}' and table_name='{tb_nm}'),
        tp_dev AS (
           select 
           column_name col_nm,
           column_default 
        from information_schema.columns 
        where   table_schema='{trgt_schm}' and table_name='{tb_nm}')
      select 
      tp_test.col_nm,
       column_default_new,
      tp_dev.column_default,'删除默认' chg_type,
        'ALTER TABLE {trgt_schm}.{tb_nm} ALTER COLUMN '||tp_test.col_nm||' DROP DEFAULT;' ddl
      from tp_test 
      left join tp_dev on tp_dev.col_nm=tp_test.col_nm
      where tp_test.column_default_new is null and tp_dev.column_default>''
      union all
       select 
      tp_test.col_nm,
       column_default_new,
      tp_dev.column_default,'修改默认' chg_type,
       'ALTER TABLE {trgt_schm}.{tb_nm} ALTER COLUMN '||tp_test.col_nm||' SET DEFAULT '||column_default_new||';' ddl
      from tp_test 
      left join tp_dev on tp_dev.col_nm=tp_test.col_nm
      where tp_test.column_default_new>'' and coalesce(tp_dev.column_default,'nulls')<>tp_test.column_default_new
    """
    check_df = conn.select(
        check_dufault_sql.format(tb_nm=trgt_tb, test_schm=test_schm, trgt_schm=trgt_schm))
    if check_df.shape[0] > 0:
        for i in check_df.index:
            logger.warning(check_df.loc[i, 'column_default'])
            logger.warning(check_df.loc[i, 'column_default_new'])
            logger.warning("您正在修改字段%s默认值，请注意确保历史数据是否需要修改或者受到影响" % check_df.loc[i, 'col_nm'])
            conn.exec(check_df.loc[i, 'ddl'], if_print_sql=False)
    logger.debug('删除测试表,完成表结构校验')
    conn.exec("DROP TABLE IF EXISTS {schm_nm}.{tb_nm};".format(schm_nm=test_schm, tb_nm=trgt_tb),
              if_print_sql=False)
    logger.debug("%s 表校验完成" % schm_tb_nm)
    return True


def get_table_pk(conn, tb_nm, schm_nm=None):
    if "." in tb_nm:
        tp = tb_nm.split(".")
        tb_nm = tp[1]
        schm_nm = tp[0]
    sql = """
     with pk as(       
             select
                 cl.oid,
                cl.relname AS tbl_name,n.nspname,
                co.conname AS pk_name,unnest(co.conkey) col_id
            FROM
                pg_constraint co
                inner join  pg_class cl on co.conrelid = cl.oid and co.contype = 'p' and cl.relname = '{tb_nm}'
                INNER JOIN pg_catalog.pg_namespace n    ON n.oid=cl.relnamespace AND n.nspname='{schm_nm}'
             )
            select pk.pk_name pk_nm,string_agg(a.attname,',') pk_cols from pk left join pg_attribute a on pk.oid=a.attrelid and a.attnum=pk.col_id
            group by pk.pk_name
    """
    sql = sql.format(tb_nm=tb_nm, schm_nm=schm_nm)
    return conn.select(sql)


def generate_simple_ddl(trgt_tb_nm, tb_strct, meta):
    col_crt = []
    col_cmnts = []
    tb_cmnt = "COMMENT ON TABLE " + trgt_tb_nm + " IS '%s';" % (tb_strct['tb_cmnt'],)
    crt_tb_sql = 'CREATE TABLE ' + trgt_tb_nm + "(\n%s \n) WITH (OIDS=FALSE);\n%s\n%s;"
    for i in meta.index:
        col_nm = meta.loc[i, 'col_nm']
        col_type = meta.loc[i, 'col_type']
        col_len = meta.loc[i, 'col_len']  # 字段长度
        col_len = 1000 if pd.isna(col_len) else int(col_len)  # 字段长度处理
        col_cmnt = meta.loc[i, 'col_cmnt'].replace("'", "").replace('"', '').replace('\n', ' ')  # 字段备注
        if meta.loc[i, 'col_type'] in ('varchar', 'bit', 'char'):
            tp = '\t"%s" %s(%d) null' % (col_nm, col_type, col_len)  # 拼接成sql
        elif meta.loc[i, 'col_type'] in ('numeric', 'decimal'):
            col_prcn = meta.loc[i, 'col_prcn']  # 精度处理
            col_prcn = 2 if pd.isna(col_prcn) else int(col_prcn)  # 精度出现nan时填补为2
            tp = '\t"%s" %s(%d,%d) null' % (col_nm, col_type, col_len, col_prcn)
        else:
            tp = '\t"%s" %s null' % (col_nm, col_type)
        col_crt.append(tp)
        col_cmnt = col_cmnt.replace("\n", "\t") if col_cmnt else col_nm
        tp = "COMMENT ON COLUMN %s.%s IS '%s'" % (trgt_tb_nm, col_nm, col_cmnt)
        col_cmnts.append(tp)
    crt_tb_sql = crt_tb_sql % (",\n".join(col_crt), tb_cmnt, ";\n".join(col_cmnts))
    # logger.info("\n" + crt_tb_sql)
    return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}


def get_table_info_sql(tb_nm, schm_nm, db_nm):
    """
    获取表结构信息的sql
    :param tb_nm:  表名 没不带schema
    :param db_schm: schema名称
    :param db_nm:  数据库名称
    :return:
    """
    sql = """with pk as(       
                 select
                     cl.oid,
                     unnest(co.conkey) col_id
                FROM
                    pg_constraint co
                    inner join  pg_class cl on co.conrelid = cl.oid and co.contype = 'p' and cl.relname = '{tb_nm}'
                    INNER JOIN pg_catalog.pg_namespace n    ON n.oid=cl.relnamespace AND n.nspname='{schm_nm}'
             )
                SELECT   distinct
                        B.relname tb_nm,
                        A.attname col_nm,
                        D.udt_name col_type,
                        A.attnum col_sort_id,
                        COALESCE(col_description(A.attrelid,A.attnum),A.attname) col_cmnt,
                        CASE WHEN D.is_nullable='YES' THEN 'Y' ELSE 'N' END col_is_null,
                        tp.description as tb_cmnt,
                        COALESCE(D.character_maximum_length,D.numeric_precision) col_len,
                        D.numeric_scale col_prcn
                        ,case when pk.col_id>0 then 'Y' else 'N' end is_pk
                    FROM pg_attribute A
                    INNER JOIN pg_catalog.pg_class B
                        ON B.oid=A.attrelid
                        AND A.attnum>0 AND B.relname='{tb_nm}'
                    INNER JOIN pg_catalog.pg_namespace C
                        ON C.oid=B.relnamespace
                        AND C.nspname='{schm_nm}'
                    INNER JOIN information_schema.columns D
                        ON D.table_name=B.relname
                        AND D.column_name=A.attname
                        AND D.table_schema=C.nspname
                        and D.table_catalog='{db_nm}'
                    left join pg_description tp on  tp.objsubid =0  and B.oid = tp.objoid
                    left join pk on pk.oid=A.attrelid and A.attnum=pk.col_id
                    order by A.attnum;"""
    sql = sql.format(tb_nm=tb_nm, db_nm=db_nm, schm_nm=schm_nm)
    return sql
# #增加：
# ALTER TABLE "mo"."mo_user"
# ADD CONSTRAINT "pk_mo_user" PRIMARY KEY ("recmo_id");
#
#
# #删除
# ALTER TABLE "mo"."mo_user"
# DROP CONSTRAINT "mo_user_pkey";
#
#
# #修改
# ALTER TABLE "mo"."mo_user"
# DROP CONSTRAINT "mo_user_pkey" ,
# ADD CONSTRAINT "pk_mo_user" PRIMARY KEY ("recmo_id");
# print(get_table_info_sql('dim_cust','edw','dpstest'))