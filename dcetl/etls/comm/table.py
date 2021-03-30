# cython: language_level=3
import os
from collections.abc import Iterator
from etls.conf.settings import TEMP_HOME, ddl_path, script_path, tb_task_info
import pandas as pd
import time
from etls.comm.conn import Conn
import logging

logger = logging.getLogger()


def get_ignore_cols(trgt_tb_nm, conn=None):
    """获取忽略导入字段"""
    if conn is None:
        conn = Conn()
    sql = """
            select 
            s.tb_nm,
            s.ignore_cols
            from {tb_task_info} s 
            where ignore_cols>'' and tb_nm='{trgt_tb_nm}'
        """
    sql = sql.format(trgt_tb_nm=trgt_tb_nm, tb_task_info=tb_task_info)
    if conn.is_exists(tb_task_info):
        df = conn.select(sql)  # pd.read_sql(sql, conn)
        if df.shape[0] > 0:
            return df.loc[0, 'ignore_cols'].split(',')  # list(df['ignore_cols'])
        else:
            return None
    else:
        return None


def crt_trgt_db_sql(tb_strct, trgt_tb_nm, trgt_db_cd="DPS"):
    """
     根据源系统表结构生成目标系统的表结构语句
    :param tb_strct:
    :param trgt_tb_nm:
    :param trgt_db_cd:
    :return:
    """
    meta = tb_strct['meta'].copy()  # 源系统表结构 类型dataframe 'col_nm', 'col_type', 'col_len', 'col_prcn', 'col_cmnt'
    from_db_type = tb_strct['db_type']
    trgt_db_conn = Conn(trgt_db_cd)
    # trgt_db_conn, trgt_db_type, trgt_db_nm = get_db_conn(trgt_db_cd)
    ignore_cols_list = get_ignore_cols(trgt_tb_nm, trgt_db_conn)  # 获取剔除的字段
    if ignore_cols_list:
        meta = meta[~meta['col_nm'].isin(ignore_cols_list)]  # 删除不必要的字段
        logger.warning(trgt_tb_nm + " 剔除不同步字段：" + ','.join(ignore_cols_list))
    from etls.comm.dbmapping import get_db_mapping
    data_type_mapping = get_db_mapping(from_db_type, trgt_db_conn.db_type)
    meta['col_type'] = meta['col_type'].apply(lambda x: data_type_mapping.get(x, data_type_mapping.get('default', x)))
    if trgt_db_conn.db_type.lower() in ["postgresql", "pg"]:
        logger.debug("转化成postgresql库")
        from etls.comm.greenplum import generate_simple_ddl
        crt_tb_sql = generate_simple_ddl(trgt_tb_nm, tb_strct, meta)
        return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
    elif trgt_db_conn.db_type.lower() in ["greenplum", "gp"]:
        logger.debug("转化成greenplum库")
        from etls.comm.greenplum import generate_simple_ddl
        crt_tb_sql = generate_simple_ddl(trgt_tb_nm, tb_strct, meta)
        return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
    else:
        raise Exception("不支持的目标数据库类型：" + trgt_db_conn.db_type)


def get_tb_sql(tb_nm, sql_type='ddl', if_sql_file_not_exists=None):
    """
    获取sql文件下的sql语句
    :rtype: str
    :param if_sql_file_not_exists: 如果sql文件不存在是否强制抛出异常，默认不抛出
    :param tb_nm: 表名称 包含schema
    :param sql_type: sql类型 ddl 建表语句 dml 数据操作语句
    :return:
    """
    # 按照schema目录的的方式存储，dml/dw/dim_date.sql
    if sql_type == 'ddl':
        sql_file_path = ddl_path
    else:
        sql_file_path = script_path
    sql_file = os.path.join(sql_file_path, tb_nm.replace(".", "/")) + ".sql"
    if not os.path.exists(sql_file):
        # 按照dml/dw.dim_date.sql查找
        sql_file = os.path.join(sql_file_path, tb_nm) + ".sql"
    if os.path.exists(sql_file):
        logger.debug("获取sql文件：%s" % (sql_file,))
        sql_file = open(sql_file, 'r')
        sql = sql_file.read()
        sql_file.close()
        return sql.strip()
    else:
        if os.path.exists(tb_nm):  # 作为sql文件路径执行
            logger.debug("获取sql文件：%s" % (tb_nm,))
            sql_file = open(tb_nm, 'r')
            sql = sql_file.read()
            sql_file.close()
            return sql.strip()
        elif os.path.exists(os.path.join(sql_file_path, tb_nm)):  # 作为sql文件路径执行
            tb_nm = os.path.join(sql_file_path, tb_nm)
            logger.debug("获取sql文件：%s" % (tb_nm,))
            sql_file = open(tb_nm, 'r')
            sql = sql_file.read()
            sql_file.close()
            return sql.strip()
        else:
            logger.warning("sql文件不存在：%s " % (sql_file,))
            if if_sql_file_not_exists == 'raise':
                logger.error("sql文件不存在：%s " % (sql_file,))
                raise Exception("sql文件不存在：%s " % (sql_file,))


def batch_dataset(dataset, batch_size):
    cache = []
    for data in dataset:
        cache.append(data)
        if len(cache) >= batch_size:
            yield cache
            cache = []
    if cache:
        yield cache


def tb_el(read_tb, read_conn, write_tb, write_conn, read_where=None, pre_sql='truncate', method='copy',
          check_tb_strct=False):
    """
        数据库导出导入
       :param check_tb_strct: 是否检查表结构
       :param method:
       :param read_tb: 读取表名
       :param read_conn: 读取的数据库标识 例如DPS
       :param write_tb: 写入的表名 例如 dw.dim_prod
       :param write_conn: 写入库的库名
       :param read_where: sql where条件
       :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
       :return:
       """
    write_conn = write_conn.upper()
    read_conn = read_conn.upper()
    if pre_sql is None or len(pre_sql) < 3 or pre_sql == 'truncate':
        pre_sql = 'truncate table ' + write_tb
        logger.info(write_conn + ' ：' + str(pre_sql))  # 默认清空表
    src_tb = Table(read_conn, read_tb)  # 源表对象
    targt_tb = Table(write_conn, write_tb)  # 目标表对象
    cols = src_tb.get_tb_cols()  # 目标表的字段
    src_cols = targt_tb.get_tb_cols()  # 源表字段
    ignore_cols_list = targt_tb.get_ignore_cols()  # 忽略字段
    cols = list(filter(lambda x: x in src_cols, cols))  # 删除源表字段不存在的字段
    if ignore_cols_list:
        cols = list(set(cols) - set(ignore_cols_list))  # 剔除忽略字段
    if check_tb_strct:
        src_tb.get_trgt_tb_crt_sql(write_tb, write_conn, check_tb_strct)
    if method == 'copy':
        try:
            start_dtm = time.time()
            file_nm = write_tb + '_' + str(int(start_dtm)) + ".csv"
            file_path = os.path.join(TEMP_HOME, file_nm)
            src_tb.tb_to_csv(file_path, columns=cols, sep='\001')  # 导出cvs
            targt_tb.csv_to_tb(file_path, columns=cols, sep='\001', pre_sql=pre_sql)  # 导入到数据表
            if os.path.exists(file_path):
                os.remove(file_path)
            return True
        except Exception as e:
            logger.warning(str(e))
            logger.warning("copy 导入失败，尝试dml的方式导入数据")
            rs = src_tb.get_data_iterator(columns=cols, where=read_where, batch_size=5000)
            targt_tb.insert(rs, columns=cols, pre_sql=pre_sql)
            return True
    else:
        rs = src_tb.get_data_iterator(columns=cols, where=read_where, batch_size=5000)
        targt_tb.insert(rs, columns=cols, pre_sql=pre_sql)
        return True


# def format_update(update):
#     param = {}
#     if isinstance(update, dict):
#         expressions = []
#         for i, k in enumerate(update):
#             param[f"u{i}"] = update[k]
#             expressions.append(f"{k}=:u{i}")
#         update = ",".join(expressions)
#     if not update:
#         raise Exception("'update' 参数不能为空值")
#     return update, param
# def format_condition(condition):
#     param = {}
#     if isinstance(condition, dict):
#         expressions = []
#         for i, k in enumerate(condition):
#             param[f"c{i}"] = condition[k]
#             expressions.append(f"{k}=:c{i}")
#         condition = " and ".join(expressions)
#     condition = f" where {condition}" if condition else ""
#     return condition, param
def check_table_struct(conn, tb_nm):
    ddl = get_tb_sql(tb_nm, sql_type='ddl', if_sql_file_not_exists=None)
    if ddl:
        conn.upd_tb_strct(ddl, schm_tb_nm=tb_nm)
        return True
    else:
        return False


class Table(object):

    def __init__(self, conn, tb_nm, cols=None, if_check_table_struct=False):
        assert isinstance(tb_nm, str)
        self.tb_nm = tb_nm
        if isinstance(conn, str):
            conn = Conn(conn)
        self.conn = conn
        if if_check_table_struct:
            check_table_struct(conn, tb_nm)
        if self.conn.is_exists(tb_nm) is False:
            rs = check_table_struct(conn, tb_nm)
            if rs is False:
                raise Exception(" %s数据库中不存在表%s" % (self.conn.db_cd, tb_nm))
        if cols:
            self.cols = cols
        else:
            self.cols = self.get_tb_cols()

    def get_tb_strct(self):
        """
        获取表结构
        :return: 表结构表
        """
        rs = self.conn.get_tb_strct(self.tb_nm)  # 获取表结构
        return rs

    def get_tb_size(self, sql=None, where=None):
        """
        获取sql取数的数据量
        :param where:  create_dtm>='2020-01-01' and flag='Y'
        :param sql:
        :return:
        """
        if self.conn.is_engine:
            if sql:  # 是否是sql语句
                if where:
                    sql = sql + ' where ' + where
                sql = "select count(1) cnt from (%s) as t" % sql
            else:
                sql = "select count(1) cnt from  %s" % self.tb_nm
                if where:
                    sql = sql + ' where ' + where
            try:
                cur = self.conn.engine.execute(sql)
                rec = cur.scalar()
                if rec:
                    return rec
                else:
                    return 0
            except Exception as e:
                logger.error(str(e))
                return None
        else:
            raise Exception("不支持的数据库类型：%s,需要单独写逻辑" % self.conn.db_type)

    def get_tb_cols(self):
        """
        获取表结构
        :return: 返回字段list
        """
        rs = self.get_tb_strct()["meta"]  # 获取表结构
        if rs.shape[0] > 0:
            cols = list(rs.loc[:, 'col_nm'])
            return cols
        else:
            return None

    def _get_insert_sql(self, columns=None):

        sql = """insert into {tb_nm} ({col_nm})
                  values ({col_values})"""
        if columns is None:
            columns = self.get_tb_cols()
        col_nm = ','.join(columns)
        col_values = ','.join(['%s'] * len(columns))
        sql = sql.format(tb_nm=self.tb_nm, col_nm=col_nm, col_values=col_values)
        return sql

    def bulk(self, records, batch_size=5000, pre_sql=None, columns=None):
        """
        批量导入
        :param columns:  指定字段 list类型
        :param records:list或者dataframe或 tuple
        :param batch_size: 批提交量
        :param pre_sql:
        :return:
        """
        if isinstance(records, pd.DataFrame):
            records = records.where(records.notnull(), None)
            records = [tuple(row) for row in records.itertuples(index=False)]
        if isinstance(records, (list, tuple, Iterator)):
            insert_sql = self._get_insert_sql(columns)
            self.conn.exec_batch(insert_sql, records, batch_size=batch_size, pre_sql=pre_sql)
            return True
        else:
            raise Exception("'params'参数类型无效")

    def insert(self, records, pre_sql=None, columns=None):
        """
        批量导入
        :param records:list或者dataframe或 tuple
        :param columns: 批提交量
        :param pre_sql:
        :return:
        """
        logger.info("%s开始插入数据" % self.tb_nm)
        if isinstance(records, pd.DataFrame):
            def _paginate(df, page_size=5000):
                """
                分页返回多个批次执行参数
                """
                page = []
                df = df.where(df.notnull(), None)
                it = df.itertuples(index=False)
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

            records = _paginate(records, page_size=10)  # [tuple(row) for row in records.itertuples(index=False)]
        if isinstance(records, (list, tuple, Iterator)):
            insert_sql = self._get_insert_sql(columns)
            conn = self.conn.get_conn_refresh()
            cursor = conn.cursor()
            try:
                if pre_sql:
                    cursor.execute(pre_sql)
                for pages in records:
                    sqls = [cursor.mogrify(insert_sql, args) for args in pages]
                    cursor.execute(b";".join(sqls))
                conn.commit()
            except Exception as e:
                logger.error(str(e))
                conn.rollback()
                raise e
            finally:
                conn.close()
                logger.info("%s完成插入数据" % self.tb_nm)
        else:
            raise Exception("'params'参数类型无效")

    def get_ignore_cols(self, trgt_tb_nm=None):
        """获取忽略导入字段"""
        if trgt_tb_nm is None:
            trgt_tb_nm = self.tb_nm
        return get_ignore_cols(trgt_tb_nm, conn=self.conn)
        # sql = """
        #         select
        #         s.tb_nm,
        #         s.ignore_cols
        #         from {tb_task_info} s
        #         where ignore_cols>'' and tb_nm='{trgt_tb_nm}'
        #     """
        # sql = sql.format(trgt_tb_nm=trgt_tb_nm, tb_task_info=tb_task_info)
        # if conn.is_exists(trgt_tb_nm):
        #     df = conn.select(sql)
        #     if df.shape[0] > 0:
        #         return df.loc[0, 'ignore_cols'].split(',')  # list(df['ignore_cols'])
        #     else:
        #         return None
        # else:
        #     return None

    def get_select_sql(self, columns=None, where=None):
        if columns is None:
            columns = self.get_tb_cols()
        cols = ",".join(columns)
        where_sql = ""
        if where:
            where_sql = "where " + where
        select_sql = "select {cols} from {tb_nm} {where_sql}".format(cols=cols, tb_nm=self.tb_nm, where_sql=where_sql)
        return select_sql

    def get_data(self, columns=None, where=None, return_dataframe=False):
        """
        :param columns: 指定字段
        :param where: where条件 sql字符串
        :param return_dataframe:
        :return: 默认返回{‘col_nm’:cols_nm list,'data':data }或者return_dataframe=True返回dataframe
        """
        if self.conn.is_engine:
            sql = self.get_select_sql(columns, where)
            return self.conn.select(sql, if_return_df=return_dataframe)

    def get_data_iterator(self, columns=None, where=None, batch_size=5000):
        """
        :param columns: 指定字段
        :param where: where条件 sql字符串
        :param batch_size:
        :return:
        """
        if self.conn.is_engine:
            sql = self.get_select_sql(columns, where)
            dataset = self.conn.engine.execute(sql)
            cache = []
            logger.info("%s 表行数：%s" % (self.tb_nm, dataset.rowcount))
            for data in dataset:
                cache.append(data)
                if len(cache) >= batch_size:
                    yield cache
                    cache = []
            if cache:
                yield cache

    def tb_to_csv(self, file_path, columns=None, where=None, sep="\011"):
        if self.conn.is_engine:
            if self.conn.db_type in ['Postgresql'.upper(), 'Greenplum'.upper()]:
                if columns is None:
                    columns = self.get_tb_cols()
                conn = self.conn.get_conn_refresh()
                try:
                    with conn.cursor() as cur:
                        with open(file_path, mode='w', encoding='utf-8') as fileObj:
                            cur.copy_to(fileObj, self.tb_nm, null='NULL', columns=columns, sep=sep)
                finally:
                    if conn:
                        conn.close()
            else:
                sql = self.get_select_sql(columns, where)
                rs = self.conn.engine.execute(sql)
                logger.info("%s 表行数：%s" % (self.tb_nm, rs.rowcount))
                with open(file_path, mode='w', encoding='utf-8') as fileObj:
                    for row in rs:
                        # n_r = ['"' + str(i).strip().replace('\r', '.').replace('\n', '.') +
                        #  '"' if i is not None else 'NULL' for i in row]
                        n_r = [str(i).strip().replace('\r', '.').replace('\n', '.') if i is not None else 'NULL' for i
                               in row]
                        line = sep.join(n_r)
                        fileObj.write(line + '\n')
        else:
            raise Exception("不支持的数据库类型")

    def csv_to_tb(self, file_path, columns=None, sep="\011", pre_sql=None, after_sql=None):
        if self.conn.is_engine:
            if self.conn.db_type == 'Postgresql'.upper():
                start_dtm = time.time()
                if columns is None:
                    columns = self.get_tb_cols()
                conn = self.conn.get_conn_refresh()
                try:
                    with conn.cursor() as cur:
                        if pre_sql:
                            cur.execute(pre_sql)
                        with open(file_path, mode='r', encoding='utf-8') as fileObj:
                            cur.copy_from(fileObj, self.tb_nm, null='NULL', columns=columns, sep=sep)
                        if after_sql:
                            cur.execute(after_sql)
                    conn.commit()
                finally:
                    if conn:
                        conn.close()
                    logger.info('数据导入: %s, 耗时: %s 秒' % (self.tb_nm, int(time.time() - start_dtm)))
            else:
                raise Exception("不支持的数据库类型")
        else:
            raise Exception("不支持的数据库类型")

    def df_insert_db(self, df, columns=None, pre_sql=None, size=4000, cols_rm_rn=None, sep='~', pk_col=None,
                     method='bulk',dtype=None):
        """
        dataframe 插入到数据库
        :param dtype:  用于to_sql,字段映射，是个数据字典。可以加快导入
        :param pk_col:  指定主键字段,多个字段用英文“,”分开
        :param df: dataframe
        :param method:  插入表方式 copy ，bulk等两种方式 bulk 适合10万以下的小表，大表请用copy
        :param columns:  插入字段
        :param pre_sql:  插入前执行的sql
        :param size:  插入的io或者批处理提交的 大小
        :param cols_rm_rn list 替换换行符或者分隔符,替换换行符性能比较低 尽量在数据库取数时就把换行符去掉
        :param sep: csv分隔符
        :return:
        """

        rows_cnt, cols_cnt = df.shape
        if rows_cnt <= 0:
            logger.info("没有数据插入")
            return True
        if self.conn.db_type in ['Postgresql'.upper(),'Greenplum'.upper()] and method == 'copy':
            self.conn.df_insert_db(df, self.tb_nm, columns, pre_sql, dtype, size, cols_rm_rn, sep, pk_col)
        else:
            if self.conn.is_engine:
                df = df.where(df.notnull(), None)  # 将nan等替换成None（插入数据库时会转换成null）
                self.bulk(df, batch_size=size, pre_sql=pre_sql)
            else:
                raise Exception("不支持的数据库类型：%s" % self.conn.db_type)

    def get_trgt_tb_crt_sql(self, trgt_tb_nm, trgt_db_cd="DPS", if_create_table=False):
        """
         根据源系统表结构生成目标系统的表结构语句,只适合简单的ods层同步表，不会获取索引、主键等信息
        :param if_create_table:  是否直接创建表
        :param trgt_tb_nm: 目标表表名 包含schema 例如dw.dim_date
        :param trgt_db_cd:
        :return:
        """
        tb_strct = self.get_tb_strct()
        # meta = tb_strct.copy()  # 源系统表结构 类型dataframe 'col_nm', 'col_type', 'col_len', 'col_prcn', 'col_cmnt'
        trgt_db_conn = Conn(trgt_db_cd)
        crt_tb_sql = crt_trgt_db_sql(tb_strct, trgt_tb_nm, trgt_db_cd=trgt_db_cd)
        if if_create_table:
            trgt_db_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
        return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
        # from_db_type = tb_strct['db_type']
        # trgt_db_conn, trgt_db_type, trgt_db_nm = get_db_conn(trgt_db_cd)
        # ignore_cols_list = self.get_ignore_cols(trgt_tb_nm)  # 获取剔除的字段
        # if ignore_cols_list:
        #     meta = meta[~meta['col_nm'].isin(ignore_cols_list)]  # 删除不必要的字段
        #     logger.warning(trgt_tb_nm + " 剔除不同步字段：" + ','.join(ignore_cols_list))
        # from etls.comm.dbmapping import get_db_mapping
        # data_type_mapping = get_db_mapping(from_db_type, trgt_db_conn.db_type)
        # meta['col_type'] = meta['col_type'].apply(
        #     lambda x: data_type_mapping.get(x, data_type_mapping.get('default', x)))
        # if trgt_db_conn.db_type.lower() in ["postgresql", "pg"]:
        #     logger.debug("转化成postgresql库")
        #     from etls.comm.greenplum import generate_simple_ddl
        #     crt_tb_sql = generate_simple_ddl(trgt_tb_nm, tb_strct, meta)
        #     if if_create_table:
        #         trgt_db_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
        #     return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
        # elif trgt_db_conn.db_type.lower() in ["greenplum", "gp"]:
        #     logger.debug("转化成greenplum库")
        #     from etls.comm.greenplum import generate_simple_ddl
        #     crt_tb_sql = generate_simple_ddl(trgt_tb_nm, tb_strct, meta)
        #     if if_create_table:
        #         trgt_db_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
        #     return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
        # else:
        #     raise Exception("不支持的目标数据库类型：" + trgt_db_conn.db_type)
        # if trgt_db_conn.db_type.lower() in ["postgresql", "pg"]:
        #     logger.debug("转化成pg库")
        #     if from_db_type.lower() == 'mysql':  # to do 作为配置文件配置
        #         type_map_mysql_to_pg = {'char': 'varchar',
        #                                 'varchar': 'varchar',
        #                                 'tinytext': 'text',
        #                                 'mediumtext': 'text',
        #                                 'text': 'text',
        #                                 'longtext': 'text',
        #                                 'tinyblob': 'bytea',
        #                                 'mediumblob': 'bytea',
        #                                 'blob': 'bytea',
        #                                 'longblob': 'bytea',
        #                                 'binary': 'bytea',
        #                                 'varbinary': 'bytea',
        #                                 'bit': 'boolean',
        #                                 'tinyint': 'smallint',
        #                                 'tinyint unsigned': 'smallint',
        #                                 'smallint': 'smallint',
        #                                 'smallint unsigned': 'integer',
        #                                 'mediumint': 'integer',
        #                                 'mediumint unsigned': 'integer',
        #                                 'int': 'integer',
        #                                 'int unsigned': 'bigint',
        #                                 'bigint': 'bigint',
        #                                 'bigint unsigned': 'numeric',
        #                                 'float': 'numeric',
        #                                 'float unsigned': 'numeric',
        #                                 'double': 'numeric',
        #                                 'double unsigned': 'numeric',
        #                                 'decimal': 'numeric',
        #                                 'decimal unsigned': 'numeric',
        #                                 'numeric': 'numeric',
        #                                 'numeric unsigned': 'numeric',
        #                                 'date': 'date',
        #                                 'datetime': 'timestamp',
        #                                 'time': 'time',
        #                                 'timestamp': 'timestamp',
        #                                 'year': 'smallint',
        #                                 'enum': 'varchar',
        #                                 'set': 'varchar',
        #                                 'bigserial': 'bigint',
        #                                 'serial': 'integer'
        #                                 }
        #         meta['col_type'] = meta['col_type'].apply(lambda x: type_map_mysql_to_pg.get(x, 'varchar'))
        #     elif from_db_type.lower() == 'postgresql' or from_db_type.lower() == 'pg':
        #         type_map_pg_to_pg = {'char': 'varchar',
        #                              'varchar': 'varchar',
        #                              'smallint': 'integer',
        #                              'int': 'integer',
        #                              'integer': 'integer',
        #                              'bigint': 'bigint',
        #                              'float': 'numeric',
        #                              'numeric': 'numeric',
        #                              'double': 'numeric',
        #                              'decimal': 'numeric',
        #                              'datetime': 'timestamp',
        #                              'bigserial': 'bigint',
        #                              'serial': 'integer'
        #                              }
        #         meta['col_type'] = meta['col_type'].apply(lambda x: type_map_pg_to_pg.get(x, x))
        #     elif from_db_type.lower() == 'oracle':
        #         type_map_oracle_to_pg = {'char': 'varchar',
        #                                  'varchar': 'varchar',
        #                                  'tinytext': 'text',
        #                                  'mediumtext': 'text',
        #                                  'text': 'text',
        #                                  'longtext': 'text',
        #                                  'tinyblob': 'bytea',
        #                                  'mediumblob': 'bytea',
        #                                  'blob': 'bytea',
        #                                  'longblob': 'bytea',
        #                                  'binary': 'bytea',
        #                                  'varbinary': 'bytea',
        #                                  'bit': 'bit',
        #                                  'tinyint': 'smallint',
        #                                  'smallint': 'smallint',
        #                                  'mediumint': 'integer',
        #                                  'int': 'integer',
        #                                  'bigint': 'bigint',
        #                                  'float': 'numeric',
        #                                  'double': 'numeric',
        #                                  'decimal': 'numeric',
        #                                  'numeric': 'numeric',
        #                                  'datetime': 'timestamp',
        #                                  'time': 'time',
        #                                  'timestamp': 'timestamp',
        #                                  'bigserial': 'bigint',
        #                                  'serial': 'integer',
        #                                  'number': 'numeric',
        #                                  'clob': 'text',
        #                                  'varchar2': 'varchar',
        #                                  'date': 'timestamp',
        #                                  'nchar': 'varchar',
        #                                  'nvarchar2': 'varchar',
        #                                  'binary_double': 'numeric',
        #                                  'binary_float': 'numeric',
        #                                  'nclob': 'text',
        #                                  'bfile': 'bytea',
        #                                  'long': 'text',
        #                                  'real': 'numeric',
        #                                  'raw': 'bytea'
        #                                  }
        #         meta['col_type'] = meta['col_type'].apply(lambda x: type_map_oracle_to_pg.get(x, 'varchar'))
        #     else:
        #         raise Exception("不支持的源数据库类型：" + tb_strct['db_type'])
        #     # trgt_meta = get_tb_strct(trgt_tb_nm, trgt_db_cd)
        #     # if trgt_meta:
        #     #     trgt_meta=trgt_meta['meta']
        #     #     # 新增字段
        #     #     meta
        #     #     print(trgt_meta)
        #     col_crt = []
        #     col_cmnts = []
        #     tb_cmnt = "COMMENT ON TABLE " + trgt_tb_nm + " IS '%s';" % (tb_strct['tb_cmnt'],)
        #     crt_tb_sql = 'CREATE TABLE ' + trgt_tb_nm + "(\n%s \n) WITH (OIDS=FALSE);\n%s\n%s;"
        #     for i in meta.index:
        #         col_nm = meta.loc[i, 'col_nm']
        #         col_type = meta.loc[i, 'col_type']
        #         col_len = meta.loc[i, 'col_len']  # 字段长度
        #         col_len = 1000 if pd.isna(col_len) else int(col_len)  # 字段长度处理
        #         col_cmnt = meta.loc[i, 'col_cmnt'].replace("'", "").replace('"', '').replace('\n', ' ')  # 字段备注
        #         if meta.loc[i, 'col_type'] in ('varchar', 'bit', 'char'):
        #             tp = '\t"%s" %s(%d) null' % (col_nm, col_type, col_len)  # 拼接成sql
        #         elif meta.loc[i, 'col_type'] in ('numeric', 'decimal'):
        #             col_prcn = meta.loc[i, 'col_prcn']  # 精度处理
        #             col_prcn = 2 if pd.isna(col_prcn) else int(col_prcn)  # 精度出现nan时填补为2
        #             tp = '\t"%s" %s(%d,%d) null' % (col_nm, col_type, col_len, col_prcn)
        #         else:
        #             tp = '\t"%s" %s null' % (col_nm, col_type)
        #         col_crt.append(tp)
        #         col_cmnt = col_cmnt.replace("\n", "\t") if col_cmnt else col_nm
        #         tp = "COMMENT ON COLUMN %s.%s IS '%s'" % (trgt_tb_nm, col_nm, col_cmnt)
        #         col_cmnts.append(tp)
        #     crt_tb_sql = crt_tb_sql % (",\n".join(col_crt), tb_cmnt, ";\n".join(col_cmnts))
        #     # logger.info("\n" + crt_tb_sql)
        #     if if_create_table:
        #         trgt_db_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
        #         trgt_db_conn.close()
        #     return crt_tb_sql  # {"tb_nm": trgt_tb_nm, 'crt_tb_sql': crt_tb_sql, 'meta': meta}
        # else:
        #     raise Exception("不支持的目标数据库类型：" + trgt_db_conn.db_type)
