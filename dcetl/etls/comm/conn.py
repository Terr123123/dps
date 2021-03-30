# cython: language_level=3
"""
Created on 2019年10月28日

@author: xuzh
"""
import pymongo
import os
import csv
import io
import re
from contextlib import contextmanager
import sqlparse
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.orm import sessionmaker
from etls.comm.loggers import get_logger
from etls.conf.settings import db_config_path, default_db_cd, dafault_tmp_schema
import configparser

logger = get_logger()
db_param = configparser.ConfigParser()
db_param.read(db_config_path, encoding='utf-8')  # 取数参数配置文件


def get_schema_and_table(schm_tb_nm):
    """根据输入的表名解析
    ：:param schm_tb_nm :dw.dim_date 解析成 dw dim_date
    """
    tp = schm_tb_nm.split(".")
    if len(tp) > 1:
        schm_nm = tp[0]
        tb_nm = tp[1]
    else:
        schm_nm = 'public'
        tb_nm = tp[0]
    return schm_nm, tb_nm


def extract_table_name_from_sql(sql_or_path):
    """
    从sql中提取表名
    :param sql_or_path:
    :return:
    """
    if os.path.exists(sql_or_path):
        sql_file = open(sql_or_path, 'r')
        sql_str = sql_file.read()
        sql_file.close()
    else:
        sql_str = sql_or_path
    # 删除comments 小写
    sql_str = sqlparse.format(sql_str, strip_comments=True, reindent=False, keyword_case='lower',
                              identifier_case='lower')
    tokens = re.split(r"[\s)(;]+", sql_str)
    result = []
    get_next = False
    for token in tokens:
        if get_next:
            if token.lower() not in ["", "select"]:
                result.append(token)
        get_next = token.lower() in ["from", "join", "into", "update"]  # 根据from join insert into update 的下一个肯定是表
    re_str = r'create table\s+(.*?)[\s(]+'
    tbs = re.findall(re_str, sql_str, re.I)
    if tbs:
        result.extend(tbs)
    re_str = r'insert\s*into\s+(.*?)[\s(]+'
    tbs = re.findall(re_str, sql_str, re.I)
    if tbs:
        result.extend(tbs)
    return list(set(result))


def get_jdbc_url(db_type, host, port, db_nm):
    """
    返回JDBC连接,用于datax交互
    :param db_type:
    :param host:
    :param port:
    :param db_nm:
    :return:
    """
    if db_type == "PostgreSQL".upper():
        jdbc = "jdbc:postgresql://{host}:{port}/{database}?connectTimeout=60&socketTimeout=60".format(host=host,
                                                                                                      port=port,
                                                                                                      database=db_nm)
    elif db_type == "Greenplum".upper():
        jdbc = "jdbc:postgresql://{host}:{port}/{database}?connectTimeout=60&socketTimeout=60".format(host=host,
                                                                                                      port=port,
                                                                                                      database=db_nm)
    elif db_type == "Oracle".upper():
        # 在oracle中 database 指的是服务名或者SID
        jdbc = "jdbc:oracle:thin:@{host}:{port}:{database}".format(host=host, port=port, database=db_nm)
    elif db_type == 'SQLServer'.upper():
        jdbc = "jdbc:sqlserver://{host}:{port};DatabaseName={database};loginTimeout=60;socketTimeout=60".format(
            host=host, port=port,
            database=db_nm)
    elif db_type == 'Mysql'.upper():
        jdbc = "jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=UTF-8&connectTimeout=60000&socketTimeout=60000".format(
            host=host,
            port=port,
            database=db_nm)
    elif db_type == 'Mongodb'.upper():
        jdbc = "jdbc:mongodb://{host}:{port}/{database}".format(host=host, port=port, database=db_nm)
    else:
        jdbc = None
    return jdbc


def get_conn(db_cd=default_db_cd):
    """
    数据库连接获取
    :param db_cd: 数据编码
    :return:
    """
    params = db_param[db_cd.lower()]
    host = params['host']
    port = params['port']
    database = params['db']
    user = params['user']
    password = params['pwd']
    db_type = params['db_type'].upper()
    if db_type == "PostgreSQL".upper():
        #  连接池容量，默认为 5，生产环境下太小，需要修改
        #  echo=True 表示打印出自动生成的 SQL 语句（通过 logging）
        conn_url = 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        return create_engine(conn_url.format(database=database, user=user, password=password, host=host, port=port),
                             pool_size=20,
                             pool_recycle=7200,
                             connect_args={'connect_timeout': 30})
    elif db_type == "Greenplum".upper():
        conn_url = 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        return create_engine(conn_url.format(database=database, user=user, password=password, host=host, port=port),
                             pool_size=20,
                             pool_recycle=7200,
                             connect_args={'connect_timeout': 30})
    elif db_type == "Mysql".upper():
        conn_url = 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8'
        return create_engine(conn_url.format(database=database, user=user, password=password, host=host, port=port),
                             pool_size=20,
                             pool_recycle=7200,
                             connect_args={'connect_timeout': 30})
    elif db_type == "Oracle".upper():
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        # dsn = cx_Oracle.makedsn(host, port, service_name=database)
        # conn = cx_Oracle.connect(user, password, dsn=dsn)
        conn_url = 'oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}'
        return create_engine(conn_url.format(database=database, user=user, password=password, host=host, port=port),
                             pool_size=20,
                             pool_recycle=7200  # ,connect_args={'timeout': 30}
                             )
    elif db_type == 'SQLServer'.upper():
        conn_url = 'mssql+pymssql://{user}:{password}@{host}:{port}/{database}?charset=utf8'
        return create_engine(conn_url.format(database=database, user=user, password=password, host=host, port=port),
                             pool_size=20,
                             pool_recycle=7200,
                             connect_args={'connect_timeout': 30})
    elif db_type == 'Mongodb'.upper():
        if len(user) > 0 and len(password) > 0:
            conn_url = 'mongodb://{user}:{password}@{host}:{port}'
        else:
            conn_url = 'mongodb://{host}:{port}'
        return pymongo.MongoClient(conn_url.format(**params))
    else:
        raise Exception("源系统%s数据库连接失败. " % db_cd)


class Conn(object):
    def __init__(self, db_cd=default_db_cd):
        if db_cd is None:
            self.db_cd = default_db_cd.lower()
        else:
            self.db_cd = db_cd.lower()
        if self.db_cd not in db_param:
            raise Exception("数据库编号：%s在db.ini中不存在" % self.db_cd)
        self.db_info = db_param[self.db_cd]
        self.host = self.db_info['host']
        self.port = self.db_info['port']
        self.db_nm = self.db_info['db']
        self.user = self.db_info['user']
        self.password = self.db_info['pwd']
        self.engine = get_conn(db_cd)
        self.is_engine = isinstance(self.engine, sqlalchemy.engine.Engine)  # 是否sqlalchemy支持的关系型数据库
        if self.is_engine:
            self.engine.raw_connection()  # 初始化时尝试连接,超时时直接报错
            # self.db_type = self.engine.dialect.name.upper()
        self.db_type = self.db_info['db_type'].upper()

    def get_session(self):
        """
        获取会话
        :return:
        """
        if self.is_engine:
            session_factory = sessionmaker(bind=self.engine)
            session = session_factory()
            return session
        else:
            raise Exception("不支持的数据库类型：%s" % self.db_type)

    def get_conn_refresh(self):
        """获取连接"""
        if self.is_engine:
            return self.engine.raw_connection()
        else:
            return self.engine

    @contextmanager
    def session_maker(self):
        """
        对会话封装,方便使用
        with session_maker() as session:
            session.execute()
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_jdbc(self):
        """
        获取jdbc URL
        :return:
        """
        jdbc = get_jdbc_url(self.db_type, self.host, self.port, self.db_nm)
        return self.db_type, self.user, self.password, jdbc

    def exec(self, sql, params=None, if_print_sql=True, if_split_to_run=False):
        """
        执行sql, ddl update insert delete
        事务处理：
         Connection 对象提供 begin() 返回 Transaction 对象。此对象通常在try/except子句中使用，以确保它可以调用 Transaction.rollback() 或 Transaction.commit() ：：

                connection = engine.connect()
                trans = connection.begin()
                try:
                    r1 = connection.execute(table1.select())
                    connection.execute(table1.insert(), col1=7, col2='this is some data')
                    trans.commit()
                except:
                    trans.rollback()
                    raise

            使用上下文管理器可以更简洁地创建上面的块，或者 Engine ：：

                # runs a transaction
                with engine.begin() as connection:
                    r1 = connection.execute(table1.select())
                    connection.execute(table1.insert(), col1=7, col2='this is some data')

        或者从 Connection ，在这种情况下， Transaction 对象也可用：

                with connection.begin() as trans:
                    r1 = connection.execute(table1.select())
                    connection.execute(table1.insert(), col1=7, col2='this is some data')
        :param if_split_to_run:
        :param sql:
        :param params: {‘a’:'fd'}或（a,b,c,d）
        :param if_print_sql: 是否大于执行sql
        :return:
        """
        sql_tp = sqlparse.format(sql, strip_comments=True)
        if sql is None or len(sql_tp) < 3:
            # logger.warning("无效sql不执行：%s" % sql)
            return
        if self.is_engine:
            conn = self.engine.raw_connection()
            try:
                with conn.cursor() as cur:
                    if if_split_to_run and ';' in sql_tp:  # 如果选择拆分执行则进行拆分
                        if params:
                            if isinstance(params, dict):
                                sql = sql.format(**params)
                            else:
                                raise Exception("拆分执行sql，传入参数只能是数据字典型，不能是list和tuple")
                        sql_list = sqlparse.split(sql)
                        for sql in sql_list:
                            if len(sqlparse.format(sql, strip_comments=True)) < 5:
                                pass  # 无效sql不执行任何操作
                            else:
                                cur.execute(sql)
                                if if_print_sql:  # 打印执行sql
                                    logger.info("执行sql:\n %s \n执行结果：%s 记录数：%d" %
                                                (sqlparse.format(sql, keyword_case='upper'), cur.statusmessage,
                                                 cur.rowcount))
                    else:
                        if params is None:
                            cur.execute(sql)
                        else:
                            if isinstance(params, dict):
                                sql = sql.format(**params)
                                cur.execute(sql)
                            else:
                                if isinstance(params, list):
                                    params = tuple(params)
                                cur.execute(sql, params)
                        if if_print_sql:  # 打印执行sql
                            logger.info("执行sql:\n %s \n执行结果：%s 记录数：%d" %
                                        (sqlparse.format(cur.query, keyword_case='upper'), cur.statusmessage,
                                         cur.rowcount))
                conn.commit()  # 提交
            except Exception as e:
                logger.error("sql执行错误：\n%s" % sql)
                conn.rollback()  # 出现错误回滚
                raise e
            finally:
                conn.close()
        elif self.db_type == "mongodb".upper():
            logger.info("mongodb 不适用，请独立写函数")

    def exec_batch(self, sql, params, batch_size=2000, pre_sql=None):
        """
        批量插入或更新，主要用于提高效率
        :param pre_sql: 执行前的sql
        :param batch_size:
        :param sql:
        :param params:
        :return:
        """
        if self.is_engine:
            def _paginate(seq, page_size):
                """
                分页返回多个批次执行参数
                """
                page = []
                it = iter(seq)
                while 1:
                    try:
                        for i in range(page_size):
                            page.append(next(it))
                        yield page
                        page = []
                    except StopIteration:
                        if page:
                            yield page
                        return

            conn = self.get_conn_refresh()
            cursor = conn.cursor()
            try:
                if pre_sql:
                    cursor.execute(pre_sql)
                for pages in _paginate(params, page_size=batch_size):  # 分页处理
                    sqls = [cursor.mogrify(sql, args) for args in pages]
                    cursor.execute(b";".join(sqls))
                conn.commit()
            except Exception as e:
                logger.error(str(e))
                conn.rollback()
            finally:
                conn.close()
        else:
            logger.error("暂时不支持的数据库类型")

    def select(self, sql, params=None, if_return_df=True):
        """
        查询sql
        :param sql:
        :param params: {‘a’:'fd'}或（a,b,c,d）
        :param if_return_df: True 返回dataframe False 返回{‘col_nm’:xx,'data':data}基本不用
        :return:
        """
        if self.is_engine:
            conn = self.get_conn_refresh()
            if params:
                if isinstance(params, dict):
                    sql = sql.format(**params)
                    if if_return_df:
                        logger.debug(sqlparse.format(sql, keyword_case='upper'))
                        conn.close()
                        return pd.read_sql(sql, conn)
                    else:
                        cur = conn.cursor()
                        cur.execute(sql)
                        logger.debug(sqlparse.format(cur.query, keyword_case='upper'))
                        col_nm = [i.name for i in cur.description]
                        data = cur.fetchall()
                        cur.close()
                        conn.close()
                        return {'col_nm': col_nm, 'data': data}
                else:
                    if isinstance(params, list):
                        params = tuple(params)
                    cur = conn.cursor()
                    cur.execute(sql, params)
                    logger.debug(sqlparse.format(cur.query, keyword_case='upper'))
                    col_nm = [i.name for i in cur.description]
                    data = cur.fetchall()
                    cur.close()
                    conn.close()
                    if if_return_df:
                        return pd.DataFrame(list(data), columns=col_nm)
                    else:
                        return {'col_nm': col_nm, 'data': data}
            else:
                # 只指定表名的sql
                if not ("select" in sql.lower()):
                    sql = "select * from " + sql
                if if_return_df:
                    return pd.read_sql(sql, conn)
                else:
                    cur = conn.cursor()
                    cur.execute(sql, params)
                    logger.debug(sqlparse.format(cur.query, keyword_case='upper'))
                    col_nm = [i.name for i in cur.description]
                    data = cur.fetchall()
                    cur.close()
                    conn.close()
                    return {'col_nm': col_nm, 'data': data}

    def query_one(self, sql, params=None):
        """
        查询sql,返回一个值。多用来查询只有一行一列的sql查询，这样就会返回一个字符或者数值，无序然后转换
        :param sql:
        :param params: {‘a’:'fd'}或（a,b,c,d）
        :return: 返回一个值 如果是多列多行则返回第1列第1行数据，如果是1列多行则返回第1行
        """
        if self.is_engine:
            if params:
                if isinstance(params, dict):
                    sql = sql.format(**params)
                    rs = self.engine.execute(sql)
                else:
                    if isinstance(params, list):
                        params = tuple(params)
                    rs = self.engine.execute(sql, params)
            else:
                rs = self.engine.execute(sql)
            rec = rs.scalar()  # first 返回首行
            if rec:
                return rec
            else:
                return None

    def make_where(self, keys):
        """
        添加where条件
        :param keys:
        :return:
        """
        if self.is_engine:
            sql_list = []
            if keys:
                sql_list.append(" where ")
                for i, key in enumerate(keys):
                    if i > 0:
                        sql_list.append(" and ")
                    sql_list.extend([key, "= %s"])
            return sql_list

    def make_select(self, table, cols=None, keys=None, orderby=None, limit=None):
        """
        产生查询语句
        :param table:
        :param cols:
        :param keys:
        :param orderby:
        :param limit: 限制数量
        :return:
        """
        if self.is_engine:
            sql_list = ["select "]
            if not cols:
                sql_list.append("*")
            else:
                if isinstance(cols, list):
                    sql_list.extend(", ".join(cols))
                else:
                    sql_list.extend(cols)
            sql_list.extend([" from ", table])
            sql_list.extend(self.make_where(keys))
            if orderby:
                sql_list.extend([" order by ", ", ".join(orderby)])
            sql = "".join(sql_list)
            if limit:
                if self.db_type.lower() in ['postgresql', 'mysql']:
                    sql = sql + " limit " + str(limit)
                elif self.db_type.lower() == 'sqlserver':
                    sql = sql.replace("select ", "select top " + str(limit) + " ")
                elif self.db_type.lower() == 'oracle':
                    if 'where ' in sql:
                        sql = sql + ' and rownum<=' + str(limit)
                    else:
                        sql = sql + ' where rownum<=' + str(limit)
                else:
                    raise Exception("limit功能不支持的数据库，请添加功能")
            return sql

    def df_insert_db(self, df, table_name, columns=None, pre_sql=None, dtype=None, size=4000, cols_rm_rn=None, sep='~',
                     pk_col=None):
        """
        dataframe 插入到数据库
        :param pk_col:  指定主键字段,多个字段用英文“,”分开
        :param df: dataframe
        :param table_name:  插入表名 例如 dw.dim_prod
        :param columns:  插入字段
        :param pre_sql:  插入前执行的sql
        :param dtype:  用于to_sql,字段映射，是个数据字典。可以加快导入
        :param size:  插入的io 大小
        :param cols_rm_rn list 替换换行符或者分隔符,替换换行符性能比较低 尽量在数据库取数时就把换行符去掉
        :param sep: csv分隔符
        :return:
        """
        rows_cnt, cols_cnt = df.shape
        if rows_cnt <= 0:
            logger.info("没有数据插入")
            return True
        df_cols = list(df.columns)  # 列
        df = df.where(df.notnull(), None)  # 替换null值 nan na nat等
        if pk_col:
            dup = df.duplicated(subset=pk_col.replace(" ", "").split())
            if dup.any():
                # 关键字段存在重复
                raise Exception("%s: 关键字段存在重复值" % pk_col)
            if not (pre_sql and pre_sql.lower().startswith("truncate")):
                dup = self.df_look_up(df, look_up_tb=table_name, where_col=pk_col.replace(" ", "").split())
                if dup['rs'].any():
                    # 主键冲突
                    raise Exception("%s: 主键冲突，在数据库中已经存在" % pk_col)
        if columns:  # 如果指定了列
            for i in columns:
                if i not in df_cols:
                    raise Exception("字段" + i + "在dataframe中不存在")
            df = df[columns]
        else:
            columns = df_cols
        if cols_rm_rn:  # 需要替换换行符的列
            for i in cols_rm_rn:
                if i in df_cols:
                    df[i] = df[i].str.replace(r'\\n', '').replace(sep, '')
        output = io.StringIO()
        if self.db_type in ['Postgresql'.upper(),'Greenplum'.upper()]:
            conn = self.get_conn_refresh()  # 获取连接
            try:
                df.to_csv(output, sep=sep, index=False, header=False, quoting=csv.QUOTE_NONNUMERIC)
                output.seek(0)  # 不包含表头的从0开始
                with conn.cursor() as cur:
                    if pre_sql:
                        logger.info(pre_sql)
                        cur.execute(pre_sql)
                        # conn.commit() 取消commit 作为一个事务处理
                    # FORCE_NULL '' 把''存储为null  # 默认 quote '\"'   指定转义 escape '\\',
                    sql = """COPY {table_name}({cols}) FROM STDIN with (FORMAT csv, DELIMITER '{seps}',FORCE_NULL({cols}), header false)"""
                    sql = sql.format(table_name=table_name, cols=','.join(columns), seps=sep)
                    logger.info("表%s 开始导入 %s 行 %s 列" % (table_name, rows_cnt, cols_cnt))
                    cur.copy_expert(sql, output, size)
                    # cur.copy_from(output, table_name, null='', sep=sep, size=size, columns=columns)
                logger.info("表%s 导入完成:%s" % (table_name, ','.join(columns)))
                conn.commit()
            except Exception as e:
                logger.info(str(e))
                conn.rollback()
                raise Exception(str(e))
            finally:
                output.close()
                conn.close()
        else:
            if self.is_engine:
                if pre_sql:
                    with self.engine.begin() as conn:
                        logger.info(pre_sql)
                        conn.execute(pre_sql)
                schm_nm, tb_nm = get_schema_and_table(table_name)
                df.to_sql(name=tb_nm, con=self.engine, if_exists='append', index=False, schema=schm_nm, dtype=dtype)
            else:
                raise Exception("不支持的数据库类型：%s" % self.db_type)

    def is_exists(self, schm_tb_nm):
        """
        判断表是否存在数据库
        :param schm_tb_nm: 表名 需要指定schema
        :return:
        """
        if "." in schm_tb_nm:
            tp = schm_tb_nm.strip().split(".")
            schm = tp[0]
            tb = tp[1]
        else:
            schm = "public"
            tb = schm_tb_nm
        if self.db_type == 'postgresql'.upper():
            check_sql = "SELECT table_name FROM information_schema.tables " \
                        "WHERE table_schema='{schm}' AND table_name='{tb}' limit 2"
            with self.get_conn_refresh().cursor() as cur:
                cur.execute(check_sql.format(schm=schm, tb=tb))
                if cur.rowcount > 0:
                    return True
                else:
                    return False
        elif self.db_type == 'ORACLE'.upper():
            sql = """
                select  t.table_name  FROM dba_tables t
                 where  t.table_name =upper('{tb_nm}') and t.owner=upper('{db_schm}')  t.status='VALID'  and rownum<2
               """.format(tb_nm=tb, db_schm=schm)
            with self.get_conn_refresh().cursor() as cur:
                cur.execute(sql)
                if cur.rowcount > 0:
                    return True
                else:
                    return False
        elif self.db_type == 'MYSQL'.upper():
            sql = """
                select t2.table_name tb_nm
                from information_schema.tables t2  
                where t2.table_name='{tb_nm}'  limit 2;""".format(tb_nm=tb)
            with self.get_conn_refresh().cursor() as cur:
                cur.execute(sql)
                if cur.rowcount > 0:
                    return True
                else:
                    return False
        elif self.db_type == 'PostgreSQL'.upper():
            sql = """    
                SELECT   distinct D.table_name tb_nm
                            FROM  information_schema.columns D
                                where D.table_name='{tb_nm}'
                                AND D.table_schema='{db_schm}'
                                and D.table_catalog='{db_nm}'"""
            sql = sql.format(tb_nm=tb, db_nm=self.db_nm, db_schm=schm)
            with self.get_conn_refresh().cursor() as cur:
                cur.execute(sql)
                if cur.rowcount > 0:
                    return True
                else:
                    return False
        elif self.db_type == 'Greenplum'.upper():
            sql = """    
                       SELECT   distinct D.table_name tb_nm
                                   FROM  information_schema.columns D
                                       where D.table_name='{tb_nm}'
                                       AND D.table_schema='{db_schm}'
                                       and D.table_catalog='{db_nm}'"""
            sql = sql.format(tb_nm=tb, db_nm=self.db_nm, db_schm=schm)
            with self.get_conn_refresh().cursor() as cur:
                cur.execute(sql)
                if cur.rowcount > 0:
                    return True
                else:
                    return False
        else:
            raise Exception("不支持的数据库类型：%s" % self.db_type)

    def csv_insert_db(self, filename, table_name, sep=',', pre_sql=None, size=4000, if_exists="append"):
        """
        CSV 插入到数据库 首行必须是列名
        :param if_exists:  {'fail', 'replace', 'append'}, default 'fail'
        :param filename: dataframe
        :param table_name:  插入表名 例如 dw.dim_prod
        :param pre_sql:  插入前执行的sql
        :param size:  插入的io 大小
        :param sep: csv分隔符
        :return:
        """
        if self.db_type in ['Postgresql'.upper(),'Greenplum'.upper()]:
            try:
                with open(filename, mode='r', encoding='utf-8') as f:
                    cols = f.readline()
                    sql = """COPY {table_name}({cols}) FROM STDIN with (FORMAT csv, DELIMITER '{seps}',FORCE_NULL({cols}), header false)"""
                    sql = sql.format(table_name=table_name, cols=cols, seps=sep)
                    conn = self.get_conn_refresh()
                    try:
                        with conn.cursor() as cur:
                            if pre_sql:
                                cur.execute(pre_sql)
                            cur.copy_expert(sql, f, size)
                            logger.info('导入数据行数：%s' % cur.rowcount)
                    finally:
                        conn.close()
            except Exception as e:
                logger.info(str(e))
                raise Exception(str(e))
        else:
            if self.is_engine:
                conn = self.get_conn_refresh()
                try:
                    if pre_sql:
                        with conn.cursor() as cur:
                            logger.info(pre_sql)
                            cur.execute(pre_sql)
                    conn.commit()
                    df = pd.read_csv(filename, sep=sep)
                    df.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=False, chunsize=size)
                finally:
                    conn.close()
            else:
                raise Exception("不支持的数据库类型：%s" % self.db_type)

    def vacuum(self, tb):
        """
        手动收集表统计信息
        :param tb:  指定表名和schema
        :return:
        """

        if self.db_type in ['Postgresql'.upper(),'Greenplum'.upper()]:
            if self.is_exists(tb):  # 判断表是否存在
                conn = self.get_conn_refresh()  # 必须获取原始连接方式
                try:
                    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    logger.info("开始手动收集{0}表统计信息".format(tb))
                    with conn.cursor() as cur:
                        cur.execute("vacuum analyze {0};".format(tb))
                    logger.info("完成手动收集{0}表统计信息".format(tb))
                finally:
                    conn.close()
        else:
            raise Exception("不支持的数据库类型：%s" % self.db_type)

    def drop_table(self, tb_nm):
        """
        删除表
        :param tb_nm:
        :return:
        """
        if self.is_engine:
            try:
                with self.engine.begin() as conn:
                    conn.execute("DROP TABLE  {tb_nm};".format(tb_nm=tb_nm))
                return True
            except Exception as e:
                logger.warning(str(e))
                return False
        else:
            raise Exception("不支持的数据库类型：%s,需要单独写逻辑" % self.db_type)

    def truncate_table(self, tb_nm):
        """
        删除表
        :param tb_nm: 可以是单个表或多个表
        :return:
        """
        if self.is_engine:
            try:
                with self.engine.begin() as conn:
                    if isinstance(tb_nm, str):
                        conn.execute("truncate table {tb_nm};".format(tb_nm=tb_nm))
                    elif isinstance(tb_nm, list):
                        for tp in tb_nm:
                            conn.execute("truncate table {tb_nm};".format(tb_nm=tp))
                return True
            except Exception as e:
                logger.warning(str(e))
                return False
        else:
            raise Exception("不支持的数据库类型：%s,需要单独写逻辑" % self.db_type)

    def get_count(self, sql_or_tbname, params=None):
        """
        获取sql取数的数据量
        :param params:
        :param sql_or_tbname:
        :return:
        """
        if self.is_engine:
            sql_or_tbname = sql_or_tbname.strip()
            if " " in sql_or_tbname:  # 是否是sql语句
                if params:
                    if isinstance(params, dict):
                        sql_or_tbname = sql_or_tbname.format(**params)
                    else:
                        sql_or_tbname = sql_or_tbname % tuple(params)
                sql = "select count(1) cnt from (%s) as t" % sql_or_tbname
            else:
                sql = "select count(1) cnt from  %s as t" % sql_or_tbname
            try:
                with self.engine.begin() as conn:
                    cur = conn.execute(sql)
                    rec = cur.scalar()
                    if rec:
                        return rec
                    else:
                        return 0
            except Exception as e:
                logger.error(str(e))
                return None
        else:
            raise Exception("不支持的数据库类型：%s,需要单独写逻辑" % self.db_type)

    def upd_tb_strct(self, crt_tb_sql, schm_tb_nm, drop_direct=False, test_schm=dafault_tmp_schema):
        """
        校验以及更新表结构
        原理：
        通过crt_tb_sql先在test_schm (schema)上建表测试表,然后测试表和目标表进行比较,最后将变化的表结构对目标表update.
        注意
        字段重命名、字段类型、字段备注、字段长度、索引变化、默认值变化等校验
        1、为了数据安全这里不对删除字段进行校验.一定要删除字段的请通过管理员删除
        2、重命名的视为先删除再新建字段(但是这里实际上是不删除而只会新建字段)
        3、修改字段默认值的请确保是否修改历史数据和其他影响
        4、表名修改视为新建表，但是不会删除老表
        目前只针对目标库为postgresql其他类型需要后续补充
        :param crt_tb_sql: 完整的建表语句
        :param schm_tb_nm: 表名 需要指定schema
        :param drop_direct: 是否直接删表重建 默认flase
        :param test_schm: #测试的scehma 默认test,先在test上建空表然后与实际的目标表表进行比对
        :return:
        """
        logger.info("%s 表结构校验(为了安全删除字段只能手动)" % schm_tb_nm)
        crt_tb_sql_lower = crt_tb_sql.lower().strip()
        if 'float' in crt_tb_sql_lower or 'double' in crt_tb_sql_lower:
            logger.warning("数据仓库中不能使用float double等类型")
            # return False
        if not ("comment on table " in crt_tb_sql_lower):
            logger.error("必须对表名注释")
            return False
        schm_tb_nm = schm_tb_nm.strip()
        if "." in schm_tb_nm:
            tp = schm_tb_nm.split(".")
            trgt_schm = tp[0]
            trgt_tb = tp[1]
        else:
            trgt_schm = "public"
            trgt_tb = schm_tb_nm
        trgt_ddl = "create table {schm}.{tb}".format(schm=trgt_schm, tb=trgt_tb)
        test_tb_nm = test_schm + "." + trgt_tb
        if not crt_tb_sql_lower.startswith(trgt_ddl):
            logger.error("建表语句和传入的数据表表名不一致")
            return False
        # conn = self.get_conn_refresh()
        if drop_direct is True or self.is_exists(schm_tb_nm) is False:
            # 直接重新建表 当表不存在时直接创建表
            drop_sql = "DROP TABLE IF EXISTS {schm}.{tb};".format(schm=trgt_schm, tb=trgt_tb)
            logger.info('删除表并重新建表或者数据库中表不存在新建表')
            logger.debug(drop_sql)
            ddl = drop_sql + "\n" + crt_tb_sql
            self.exec(ddl, if_print_sql=False)
            return True
        else:
            if self.db_type == 'Postgresql'.upper():
                from etls.comm.postgresql import update_table_info
                update_table_info(self, crt_tb_sql, schm_tb_nm, test_tb_nm, test_schm, trgt_tb, trgt_schm)
            elif self.db_type == 'Greenplum'.upper():
                from etls.comm.greenplum import update_table_info
                update_table_info(self, crt_tb_sql, schm_tb_nm, test_tb_nm, test_schm, trgt_tb, trgt_schm)
            else:
                raise Exception("不支持的数据库类型：%s" % self.db_type)

    def exec_sql_file(self, sql_file_path, params=None):
        """
        获取sql文件下的sql语句
        :param params:
        :param sql_file_path: 如果sql文件不存在是否强制抛出异常，默认不抛出
        :return:
        """
        if self.is_engine:
            if os.path.exists(sql_file_path):
                sql_file = open(sql_file_path, 'r')
                sql = sql_file.read()
                sql_file.close()
                self.exec(sql, params)
            else:
                raise Exception("sql文件不存在：%s " % (sql_file_path,))
        else:
            raise Exception("不支持的数据库类型：%s" % self.db_type)

    def close(self):
        # 关闭连接池
        if self.is_engine:
            self.engine.dispose()
        else:
            self.engine.close()

    def get_tb_strct(self, tb_nm):
        """
        获取源库的表结构
        :param tb_nm:
        :return:
        """
        # conn, db_type, db_nm = get_db_conn(db_cd)
        if "." in tb_nm:
            tp = tb_nm.split(".")
            db_schm = tp[0]
            tb_nm = tp[1]
        else:
            db_schm = 'public'
        if self.db_type.upper() == 'ORACLE'.upper():
            from etls.comm.oracle import get_table_info_sql
            sql = get_table_info_sql(tb_nm, db_schm)
        elif self.db_type.upper() == 'MYSQL'.upper():
            from etls.comm.mysql import get_table_info_sql
            sql = get_table_info_sql(tb_nm, self.db_nm)
        elif self.db_type.upper() == 'PostgreSQL'.upper():
            from etls.comm.postgresql import get_table_info_sql
            sql = get_table_info_sql(tb_nm, db_schm, self.db_nm)
        elif self.db_type.upper() == 'Greenplum'.upper():
            from etls.comm.greenplum import get_table_info_sql
            sql = get_table_info_sql(tb_nm, db_schm, self.db_nm)
            # sql = sql.format(tb_nm=tb_nm, db_nm=self.db_nm, db_schm=db_schm)
        elif self.db_type.upper() == 'SQLServer'.upper():
            from etls.comm.sqlserver import get_table_info_sql
            sql = get_table_info_sql(tb_nm=tb_nm, db_nm=self.db_nm)
        else:
            # logger.error("不支持的数据库类型：" + self.db_type)
            raise Exception("不支持的数据库类型：" + self.db_type)
        # logger.warning(sql)
        meta = self.select(sql)  # pd.read_sql(sql, conn)
        if meta.shape[0] > 0:
            rs = {}
            meta.columns = (map(lambda x: x.lower(), list(meta.columns)))
            tb_cmnt = meta.loc[0, 'tb_cmnt']
            meta['col_nm'] = meta['col_nm'].str.lower()
            meta['tb_nm'] = meta['tb_nm'].str.lower()
            meta['col_type'] = meta['col_type'].str.lower()
            meta['col_len'] = meta['col_len'].fillna(1000)
            meta['col_len'] = meta['col_len'].astype(int)
            meta['col_cmnt'] = meta['col_cmnt'].fillna('无列备注')
            rs['tb_cmnt'] = tb_cmnt if tb_cmnt and len(tb_cmnt) > 0 else tb_nm
            rs['tb_nm'] = tb_nm
            rs['db_type'] = self.db_type
            rs['db_cd'] = self.db_cd
            rs['meta'] = meta[['col_nm', 'col_type', 'col_len', 'col_prcn', 'col_cmnt']]  # dataframe
            return rs
        else:
            logger.warning("%s表不存在 %s" % (tb_nm, self.db_cd))
            return None

    def get_tb_cols(self, tb_nm):
        """
        获取表结构
        :param tb_nm:
        :return:
        """
        rs = self.get_tb_strct(tb_nm)["meta"]  # 获取表结构
        if rs.shape[0] > 0:
            cols = list(rs.loc[:, 'col_nm'])  # .loc[: ,'col_nm']
            return cols  # '"' + '","'.join(cols) + '"'
        else:
            return None

    def copy_to_csv(self, file, table, sep='\t', null='\\N', columns=None):
        if self.db_type == "PostgreSql".upper():
            conn = self.get_conn_refresh()  # 获取连接
            with conn:
                cur = conn.cursor()
                cur.copy_to(file, table, sep, null, columns)

    def extract_tb_from_sql(self, sql_file_path, params=None):
        """
        获取sql文件下的sql语句
        :param params:
        :param sql_file_path: 如果sql文件不存在是否强制抛出异常，默认不抛出
        :return:
        """
        if self.is_engine:
            if os.path.exists(sql_file_path):
                sql_file = open(sql_file_path, 'r')
                sql = sql_file.read()
                sql_file.close()
            else:
                sql = sql_file_path
                if params:
                    sql = sql.format(**params)
            return extract_table_name_from_sql(sql)
        else:
            raise Exception("不支持的数据库类型：%s" % self.db_type)

    def look_up_from_db(self, tb_nm, where_col, where_val, look_up_col=None):
        """
        查询表
        :param look_up_col:  查询的字段
        :param tb_nm:
        :param where_col:
        :param where_val:
        :return:
        """
        sql = self.make_select(tb_nm, look_up_col, where_col, limit=1)
        rs = self.select(sql, where_val, if_return_df=False)
        if rs['data']:
            return rs['data'][0]
        return None

    def df_look_up(self, df, look_up_tb, where_col, look_up_col="True as rs"):
        """
        数据框look_up
        :param df:
        :param look_up_tb:
        :param where_col:
        :param look_up_col:
        :return:
        """
        df = df.copy()
        tb_size = self.get_count(sql_or_tbname=look_up_tb)
        rows_cnt, cols_cnt = df.shape
        if isinstance(look_up_col, (list, tuple)):
            look_up_col_new = []
            for i in look_up_col:
                if i in df.columns:
                    i = str(i) + '_lkup'
                look_up_col_new.append(i)
        elif look_up_col == "True as rs":
            look_up_col_new = ['rs']
        else:
            look_up_col_new = [look_up_col + "_lkup"]

        if tb_size <= 0:
            for i in look_up_col_new:
                if look_up_col == "True as rs":
                    df.loc[:, i] = False
                else:
                    df.loc[:, i] = None
        elif rows_cnt < 5000 or tb_size > 100000:
            def get_look_rs(row):
                where_val = []
                for j in where_col:
                    where_val.append(row[j])
                tp = self.look_up_from_db(look_up_tb, where_col, where_val, look_up_col)
                if tp:
                    return tp
                else:
                    if look_up_col == "True as rs":
                        return False
                    elif isinstance(look_up_col, (list, tuple)):
                        return tuple([None] * len(look_up_col))
                    else:
                        return None

            df[look_up_col_new] = df.apply(get_look_rs, axis=1, result_type="expand")
        else:
            if isinstance(look_up_col, (list, tuple)):
                look_up_col.extend(where_col)
            elif isinstance(look_up_col, str):
                look_up_col = [look_up_col]
                look_up_col.extend(where_col)
            look_up_col = list(set(look_up_col))
            sql = self.make_select(look_up_tb, look_up_col)
            lkup_df = self.select(sql, if_return_df=True)
            df = df.merge(lkup_df, how='left', on=where_col, suffixes=('', '_lkup'))
            if 'rs' in df.columns:
                df.fillna({'rs': False}, inplace=True)

        return df

    def __enter__(self):
        return self

    def __exit__(self, types, value, traceback):
        """
        @summary: 会话管理器在代码块执行完成好后调用（不同于__del__）(必须是4个参数)
        """
        logger.debug("__exit__:Close %s %s %s" % (types, value, traceback))
        self.close()


def run_test():
    conn2 = Conn('DPS')
    print(conn2.query_one("select cust_nm from dw.dim_cust limit 1"))
    df = conn2.select("select emp_cd,emp_nm,emp_stat from dw.dim_emp limit 100")
    parm = df.values
    print(parm)
    conn2.exec_batch("update dw.dim_emp set emp_nm = %s,emp_stat= %s where emp_cd= %s", parm, 1000)


if __name__ == '__main__':
    run_test()
