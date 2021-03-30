# cython: language_level=3
# encoding: utf-8
from etls.conf.settings import TEMP_HOME, datax_path, ddl_path, script_path, etl_root_module, default_db_cd, \
    tb_task_info, tb_task_log, data_file_path
from etls.comm.conn import Conn, extract_table_name_from_sql
import json
import multiprocessing
from etls.comm.loggers import get_logger, get_handler
from etls.comm.batch import BatchParams
import re
import importlib
import inspect
import subprocess
import os
import time
from etls.comm.datefunc import Date as BatchDate
from etls.comm.emailcomm import send_error_msg
from etls.comm.table import tb_el, get_ignore_cols, crt_trgt_db_sql

logger = get_logger()
datax_path = datax_path + '/bin/datax.py'
path_ddl_sql = ddl_path  # ddl表定义sql路径
path_dml_sql = script_path  # dml表执行sql路径
datax_job_json_path = os.path.join(data_file_path, 'flows')


# 为新调度系统添加默认参数，如果没有指定变量的数值，用以下数据代替
# 暂时放在这里以后移到setting.py 文件中
# etl_params = {  # todo 每个批次生成一个参数文件 jinja2sql
#     'p_days': 1000,  # 用于增量天数,默认是增量处理最近1000天,
#     'etl_home': BASE_DIR,  # etl根目录
# }


def get_base_module():
    """获取顶层模块，dpsetl 也可以放在setting.py中配置"""
    # base_module = []
    # for i in sys.modules.items():  # 获取当前所有可用模块
    #     if BASE_DIR in str(i[1]) and "_" not in i[0]:
    #         base_module.append(i[0].split(".")[0])
    # from collections import Counter
    # return Counter(base_module).most_common(2)[0][0]
    return etl_root_module


def parse_sql_param(sql):
    """
    解析sql语句中的变量参数，返回参数列表crt_el_log_tb
    :param sql:
    :return:
    """
    if sql and len(sql) > 4:
        p1 = re.compile(r'[{](\S*)[}]')  # 最小匹配 \S 任意非空字符 \w排除了空格 匹配字母数字及下划线，即a-z、A-Z、0-9、_
        params = re.findall(p1, sql)
        return list(set(params))
    else:
        return []


def parse_sql_replace_param(sql, params):
    """
    替换sql中的变量
    :param sql:
    :param params:
    :return:
    """
    sql_params = parse_sql_param(sql)  # 解析sql中获取变量
    if sql_params:
        if params is None:  # 没有传入params参数时
            params = {}
        try:
            # 读取参数文件
            batch_param = BatchParams(params.get('batch_id', '0'), params)
            etl_params = batch_param.read()
            sys_params = BatchDate(etl_params['batch_dt'])  # 以batch_dt作为参数初始化系统变量
            for ky in sql_params:  # 从sql变量中获取系统参数
                sql = sql.replace("${" + ky, "{" + ky)  # 将${name}变量变成{name}
                if ky.startswith('sys_'):  # 系统变量参数以sys_开头
                    # sys_this_mon 获取BatchDate this_mon的函数值
                    sys_key = ky[4:]  # 把sys_截断
                    params[ky] = sys_params.get_param(sys_key)
                elif ky not in params:  # 有变量的但是没有传入值的
                    if ky in etl_params:
                        params[ky] = etl_params.get(ky)
            sql = sql.format(**params)
            return sql
        except Exception as e:
            err_msg = "sql文件的变量参数替换失败:" + str(e) + " with params:" + str(params)
            logger.error(err_msg)
            raise Exception(err_msg)
    else:
        return sql


def get_cols(db_cd, tb_nm):
    """
    获取字段名
    :param db_cd: 只是编号
    :param tb_nm:
    :return:
    """
    conn = Conn(db_cd)
    rs = conn.get_tb_strct(tb_nm)  # 获取表结构
    if rs:
        rs = rs['meta']['col_nm']
        cols = list(rs)
        return cols  # '"' + '","'.join(cols) + '"'


def get_sql(tb_nm, sql_type='ddl', if_sql_file_not_exists=None):
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
        sql_file_path = path_ddl_sql
    else:
        sql_file_path = path_dml_sql
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


def get_denpend(tb_nm):
    """
    自动获取获取作业依赖
    :param tb_nm:
    :return:
    """
    sql = get_sql(tb_nm, sql_type='dml', if_sql_file_not_exists='raise')
    tb = extract_table_name_from_sql(sql)
    depend = [i for i in tb if len(i) > 4 and not i.startswith('tp') and not i.startswith('temp')]
    return depend


def json_file(read_tb, read_conn, write_tb, write_conn, read_where='', pre_sql='truncate', parallel_num=1):
    """
    创建datax执行的json文件
    :param read_tb:  源表
    :param read_conn: 源系统连接
    :param write_tb:   目标表
    :param write_conn:  目标系统连接
    :param read_where:  where过滤
    :param pre_sql:   执行前执行sql
    :param parallel_num: 进程数 channel
    :return:
    """
    if os.path.exists(os.path.join(datax_job_json_path, write_tb + ".json".lower())):
        # 如果已经存在json配置文件则直接返回
        return os.path.join(datax_job_json_path, write_tb + ".json".lower())
    if pre_sql == 'truncate':
        pre_sql = 'truncate table ' + write_tb
    src_conn = Conn(read_conn)
    read_db_type, read_user, read_pwd, read_jdbc = src_conn.get_jdbc()
    if read_db_type == "Greenplum".upper():
        # greeenplum 读 走postgresql线路
        read_db_type = "Postgresql"
    trgt_conn = Conn(write_conn)
    write_db_type, write_user, write_pwd, write_jdbc = trgt_conn.get_jdbc()
    if write_db_type == "Greenplum".upper():
        # greeenplum 写
        write_db_type = "gpdb"
    cols = get_cols(write_conn, write_tb)
    ignore_cols_list = get_ignore_cols(write_tb, trgt_conn)
    if ignore_cols_list:
        cols = list(set(cols) - set(ignore_cols_list))
    src_cols = get_cols(read_conn, read_tb)
    cols = list(filter(lambda x: x in src_cols, cols))  # 过滤不需要的字段
    jsons = {
        "job": {
            "content": [
                {
                    "reader": {
                        "name": read_db_type.lower() + "reader",
                        "parameter": {
                            "username": read_user,
                            "password": read_pwd,
                            "column": cols,
                            "where": read_where,
                            "connection": [
                                {
                                    "table": [read_tb],
                                    "jdbcUrl": [read_jdbc]
                                }
                            ]
                        }
                    },
                    "writer": {
                        "name": write_db_type.lower() + "writer",
                        "parameter": {
                            "username": write_user,
                            "password": write_pwd,
                            "column": cols,
                            "preSql": [pre_sql],
                            "connection": [
                                {
                                    "jdbcUrl": write_jdbc,
                                    "table": [write_tb]
                                }
                            ]
                        }
                    }
                }
            ],
            "setting": {
                "speed": {
                    "channel": parallel_num
                }
            }
        }
    }
    try:
        datax_json_path = os.path.join(TEMP_HOME, write_tb + ".json".lower())
        datax_json_file_handler = open(datax_json_path, "w")
        json_str = json.dumps(jsons, indent=4, sort_keys=False, ensure_ascii=False)
        logger.debug("\n" + json_str.replace(read_pwd, '*****').replace(write_pwd, '******'))
        datax_json_file_handler.write(json_str)
        logger.info('datax json 文件生成：' + datax_json_path)
        return datax_json_path
    except Exception as e:
        logger.error('datax json 文件生成错误：' + str(e))


def exec_shell(shell, logs_print=False):
    try:
        proc = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while proc.poll() is None:
            line = proc.stdout.readline().strip().decode('utf-8')
            if logs_print:
                logger.info(line)
    except Exception as e:
        logger.info('shell执行失败：' + str(e))
        raise Exception("shell执行失败:" + str(e))


def datax(read_tb, read_conn, write_tb, write_conn='DPS', read_where='', pre_sql='truncate', parallel_num=2,
          check_tb_strct=True,
          logs_print=True, timeout=1800):
    """
    datax数据导入转换
    :param read_tb: 读取表名
    :param read_conn: 读取的数据库标识 例如DPS CRM PFUND
    :param write_tb: 写入的表名 例如 dw.dim_prod
    :param write_conn: 写入库的库名 例如DPS CRM PFUND
    :param read_where: sql where条件
    :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
    :param parallel_num: 并发的channel 数
    :param check_tb_strct: 是否检验表结构
    :param logs_print 是否打印日志到终端展示 不管是否设置 日志都会存储到datax/log路径下
    :param timeout 超时执行 秒，认执行30分钟则超时
    :return:
    """
    write_conn = write_conn.strip().upper()
    read_conn = read_conn.strip().upper()
    read_tb = read_tb.strip()  # 去除空格
    write_tb = write_tb.strip()  # 去除空格
    parallel_num = int(parallel_num)
    logger.info('datax数据开始导入:' + write_tb)
    if read_where is None or len(read_where) < 3:
        read_where = ''  # 默认不过滤数据
    if pre_sql is None or len(pre_sql) < 3:
        pre_sql = 'truncate'  # 默认清空表
    if 'DATAX_HOME' not in os.environ:
        logger.warning('DATAX_HOME 环境变量未设置，请设置$DATAX_HOME 并设置PATH:=$PATH:$DATAX_HOME/bin')
        shells = datax_path + ' '
    else:
        shells = 'datax.py '
    try:
        if check_tb_strct:
            to_check_tb_strct(src_tb_nm=read_tb, src_db_cd=read_conn, trgt_tb_nm=write_tb, trgt_db_cd=write_conn)
            # targt_conn = get_conn(write_conn)
            # to_check_tb_strct(targt_conn, write_tb)
        json_path = json_file(read_tb, read_conn, write_tb, write_conn, read_where, pre_sql, parallel_num)
        shells = shells + json_path
        logger.info('导入shell:' + shells)
        if os.name == 'nt':  # nt 表示Windwos系操作系统， posix 代表类Unix或OS X系统。
            proc = subprocess.Popen('python ' + shells, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        else:
            proc = subprocess.Popen(shells, shell=True, bufsize=10240, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    preexec_fn=os.setpgrp)
        pre_line = ''
        error_line = ''
        error_flag = "ERROR:"
        if_error = False
        start_ts = time.time()
        while proc.poll() is None:
            if time.time() - start_ts > timeout:  # 超时配置
                os.killpg(proc.pid, 9)  # 无条件结束
                # 当shell=True时，只有os.killpg才能kill子进程  proc.terminate() 不能
                raise TimeoutError("%s 执行超过%d秒" % (shells, timeout))
            line = proc.stdout.readline().strip().decode('utf-8')
            if line and len(line) > 0:
                pre_line = line
                if line.find(error_flag) > 0:
                    error_line = line
                    if_error = True
                if logs_print:
                    logger.info(line)
        # proc.stdout.close()
        if proc.returncode == 0 and if_error is False:
            logger.info('datax数据导入完成：' + write_tb)
            os.remove(json_path)
            return True
        else:
            logger.error(write_tb + '同步错误ERROR')
            try:
                os.killpg(proc.pid, 9)  # 无条件结束
            except Exception as e:
                logger.info(str(e))
            finally:
                raise Exception('表%s 同步错误：%s \n %s' % (write_tb, error_line, pre_line))
    except Exception as e:
        # os.kill(proc.pid, -9)
        logger.info('datax 数据导入失败：' + str(e))
        raise Exception(str(e))


def pypd(read_tb, read_conn, write_tb, write_conn='DPS', read_where=None, pre_sql='truncate', check_tb_strct=True):
    """
       datax启动需要10秒针对小表不划算，用此方法更快，但是此方法有一点瑕疵：int型目标字段，
       如果源表int型字段有缺失值，则在pandas中会自动转化为numeiric类型，插入时会报错，所以使用时特别注意。
       将可能的出现缺失的int型字段改为numeric(10,0) 即没有小数位的数值型
       :param read_tb: 读取表名
       :param read_conn: 读取的数据库标识 例如DPS
       :param write_tb: 写入的表名 例如 dw.dim_prod
       :param write_conn: 写入库的库名
       :param read_where: sql where条件
       :param check_tb_strct: 是否检验表结构
       :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
       :return: Boolean
       """
    logger.info('pypd数据开始导入:' + write_tb)
    write_conn = write_conn.upper()
    read_conn = read_conn.upper()
    try:
        if check_tb_strct:
            to_check_tb_strct(src_tb_nm=read_tb, src_db_cd=read_conn, trgt_tb_nm=write_tb, trgt_db_cd=write_conn)
        tb_el(read_tb, read_conn, write_tb, write_conn, read_where=read_where, pre_sql=pre_sql, method='copy')
        logger.info('pypd数据导入完成:' + write_tb)
        return True
    except Exception as e:
        logger.info('pypd 数据导入失败：' + str(e))
        raise Exception(write_tb + ':' + str(e))
        # return False


def to_check_tb_strct(src_tb_nm=None, src_db_cd=None, trgt_tb_nm=None, trgt_db_cd="DPS", if_ddl_not_exists="raise"):
    """
    校验表结构
    :param if_ddl_not_exists:
    :param src_tb_nm:
    :param src_db_cd:
    :param trgt_tb_nm:
    :param trgt_db_cd:
    :return:
    """
    sql = get_sql(trgt_tb_nm, sql_type='ddl')  # 获取表结构的sql文件
    if sql:  # 存在sql文件时，以sql文件为主
        if isinstance(trgt_db_cd, str):
            conn = Conn(trgt_db_cd)  # get_conn(trgt_db_cd)
        else:
            conn = trgt_db_cd
        check_rs = conn.upd_tb_strct(sql, trgt_tb_nm)
        if check_rs is False:
            raise Exception("表结构校验不通过，请查看上面的错误明细")
    elif src_tb_nm and src_db_cd:
        err_msg = "根目录下的sql/ddl/" + trgt_tb_nm.replace(".", "/") + ".sql文件不存在，从源系统拉取表结构校验"
        logger.warning(err_msg)
        src_tb_sync_ods(src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd, if_el_data=False)  # 校验表结构并修改
    else:
        err_msg = "根目录下的sql/ddl/" + trgt_tb_nm.replace(".", "/") + ".sql 文件不存在，无法校验"
        logger.warning(err_msg)
        if if_ddl_not_exists == "raise":
            # send_error_msg(err_msg, trgt_tb_nm)
            raise Exception(err_msg)


def crt_el_log_tb(conn):
    """
    创建日志表
    :param conn:
    :return:
    """
    crt_tb_sql = """
        CREATE TABLE {schm_tb_nm} (
            id serial  , -- ID
            batch_id varchar(50) not NULL, -- 任务批次号
            tb_nm varchar(100) not NULL, -- 同步表名称
            batch_stat int NULL, -- 任务状态 0 任务开始 1 任务成功 2 任务失败
            start_dt timestamp NULL, -- 任务开始时间
            end_dt timestamp NULL, -- 任务结束时间
            error_msg text NULL -- 任务失败信息
        )
        WITH (
            OIDS=FALSE
        ) ;
        CREATE INDEX etl_logs_batch_id_idx ON {schm_tb_nm} USING btree (batch_id, tb_nm) ;
        CREATE INDEX etl_logs_start_dt_idx ON {schm_tb_nm} USING btree (start_dt) ;
        -- Column comments
        comment on table {schm_tb_nm} is 'ods表同步任务日志';
        COMMENT ON COLUMN {schm_tb_nm}.id IS 'ID' ;
        COMMENT ON COLUMN {schm_tb_nm}.batch_id IS '任务批次号' ;
        COMMENT ON COLUMN {schm_tb_nm}.tb_nm IS '同步表名称' ;
        COMMENT ON COLUMN {schm_tb_nm}.batch_stat IS '任务状态0:任务开始 1:任务成功 2:任务失败' ;
        COMMENT ON COLUMN {schm_tb_nm}.start_dt IS '任务开始时间' ;
        COMMENT ON COLUMN {schm_tb_nm}.end_dt IS '任务结束时间' ;
        COMMENT ON COLUMN {schm_tb_nm}.error_msg IS '任务失败信息' ;
        """
    if not conn.is_exists(tb_task_log):
        crt_tb_sql = crt_tb_sql.format(schm_tb_nm=tb_task_log)
        rs = conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=tb_task_log, drop_direct=False)
        return rs


def if_batch_exists(batch_id, conn):
    """

    :param batch_id:
    :param conn: Conn类
    :return:
    """
    # if conn is None:
    #     conn = get_conn()
    sql = "select batch_id,batch_stat stat from {log_tb_nm} where batch_id='{batch_id}'  limit 3"
    sql = sql.format(batch_id=batch_id, log_tb_nm=tb_task_log)
    df = conn.select(sql)  # pd.read_sql(sql, conn)
    if df.shape[0] > 0:
        return True
    else:
        return False


def get_error_job(batch_id, conn, batch_nm, check_error_only=False):
    """
    :param check_error_only: 是否只检查执行错误的作业，如果是则不判断是否有作业没有被执行
    :param batch_nm:
    :param batch_id:
    :param conn: Conn 类
    :return:
    """
    # if conn is None:
    #     conn = get_conn()
    if if_batch_exists(batch_id, conn) is False:
        logger.info("%s batch_id不存在" % (batch_id,))
        return None
    if check_error_only:
        sql = """
        select 
        batch_id,
        tb_nm,
        error_msg,
        start_dt 
        from {log_tb_nm} 
            where batch_id='{batch_id}' and batch_stat=2  
        """
        sql = sql.format(batch_id=batch_id, log_tb_nm=tb_task_log)
    else:
        sql = """
               select 
               batch_id,
               tb_nm,
               error_msg,
               start_dt 
               from {log_tb_nm} 
                   where batch_id='{batch_id}' and batch_stat=2 -- and error_msg>''
               union all
               select 
               s.tb_nm_cn batch_id, 
               s.tb_nm,
               case when l.batch_stat is null then '作业没有执行' else '作业执行超时' end  error_msg,
               null start_dt
               from {tb_task_info} s 
               left join {log_tb_nm}  l on l.batch_id='{batch_id}' and l.tb_nm=s.tb_nm
               where is_del='N' and batch_nm='{batch_nm}' and (l.batch_stat is null or l.batch_stat=0) 
           """
        sql = sql.format(batch_id=batch_id, log_tb_nm=tb_task_log, batch_nm=batch_nm, tb_task_info=tb_task_info)
    err_df = conn.select(sql)  # pd.read_sql(sql, conn)
    if err_df.shape[0] > 0:
        return err_df
    else:
        return None


def is_runing(tb_nm, conn=None, batch_id=''):
    """
    :param batch_id:
    :param tb_nm:
    :param conn:
    :return:
    """
    if conn is None:
        conn = Conn()
    sql = """select batch_id from {log_tb_nm} where tb_nm='{tb_nm}' 
                and start_dt>= current_timestamp-interval '3 hour' and batch_stat=0"""
    sql = sql.format(log_tb_nm=tb_task_log, tb_nm=tb_nm)
    flag = conn.query_one(sql)  # pd.read_sql(sql, conn)
    times = 0
    while flag:
        logger.warning("%s在执行中本次执行开始等待，等待第%s次" % (tb_nm, times))
        send_error_msg("batch_id:%s的作业%s一直在执行中本批次%s等待，请确认是否有执行故障" % (flag, batch_id, tb_nm), tb_nm, if_to_wx=False)
        time.sleep(300)  # 等待5分钟
        times += 1
        flag = conn.query_one(sql)
        if flag and times > 3:
            logger.error("%s一直在执行中，请确认是否产生故障。此次执行报错" % tb_nm)
            raise Exception("%s一直在执行中，请确认是否产生故障。此次执行报错" % tb_nm)
    return False


def get_el_tb_job_stat(batch_id, tb_nm, conn=None):
    """
    :param batch_id:
    :param tb_nm:
    :param conn:
    :return:
    """
    if conn is None:
        conn = Conn()
    sql = """select batch_id,tb_nm,batch_stat stat from {log_tb_nm} 
             where batch_id='{batch_id}' and tb_nm='{tb_nm}' order by start_dt desc limit 1"""
    sql = sql.format(batch_id=batch_id, log_tb_nm=tb_task_log, tb_nm=tb_nm)
    df = conn.select(sql)  # pd.read_sql(sql, conn)
    if df.shape[0] > 0:
        return df.loc[0, 'stat']
    else:
        return -1


def el_start_stat(conn, batch_id, tb_nm):
    """
    任务开始状态
    :param conn:
    :param batch_id:
    :param tb_nm:
    :return:
    """
    stat = get_el_tb_job_stat(batch_id, tb_nm, conn)
    if stat < 0:
        batch_stat = 0  # 0 表示任务开始
        sql = """insert into {log_tb_nm} (batch_id,tb_nm,batch_stat,start_dt) values('{batch_id}','{tb_nm}',
                {batch_stat},current_timestamp)"""
        sql = sql.format(log_tb_nm=tb_task_log, batch_id=batch_id, tb_nm=tb_nm, batch_stat=batch_stat)
        conn.exec(sql, if_print_sql=False)


def el_upd_stat(conn, batch_id, tb_nm, batch_stat, error_msg=''):
    """
    更新el作业状态
    :param conn:
    :param batch_id: 批次号
    :param tb_nm:       同步表名称
    :param batch_stat:    0 表示任务开始; 1 表示完成 成功 ;2 表示 失败 完成
    :param error_msg:  错误信息
    :return:
    """

    if len(error_msg) > 2:
        sql = """update {log_tb_nm} set batch_stat= %s,end_dt=current_timestamp,error_msg= %s
                    where  tb_nm= %s and batch_id= %s ;"""
        param = (batch_stat, error_msg, tb_nm, str(batch_id))
    else:
        sql = """update {log_tb_nm} set batch_stat= %s,end_dt=current_timestamp
                            where  tb_nm= %s and batch_id= %s ;"""
        param = (batch_stat, tb_nm, str(batch_id))
    sql = sql.format(log_tb_nm=tb_task_log)
    # sql = sql.format(log_tb_nm=log_tb_nm, batch_id=batch_id, tb_nm=tb_nm, batch_stat=batch_stat, error_msg=error_msg)
    conn.exec(sql, param, if_print_sql=False)


def run_el(el_type, read_tb, read_conn, write_tb, write_conn='DPS', read_where='', pre_sql='truncate',
           parallel_num=2, check_tb_strct=True, logs_print=True, batch_dt=None):
    """
    同步表数据
    :param el_type:  datax或者pypd
    :param read_tb: 读取表名
    :param read_conn: 读取的数据库标识 例如DPS CRM PFUND
    :param write_tb: 写入的表名 例如 dw.dim_prod
    :param write_conn: 写入库的库名 例如DPS CRM PFUND
    :param read_where: sql where条件
    :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
    :param parallel_num: 并发的channel 数
    :param check_tb_strct: 是否检验表结构
    :param logs_print 是否打印日志到终端展示 不管是否设置 日志都会存储到datax/log路径下
    :param batch_dt: 批次日期 如果带有系统变量需要可以指定这个值
    :return:
    """
    if write_tb is None:
        write_tb = get_targt_tb_nm(read_tb, read_conn)
    # try:
    if read_where:
        read_where = parse_sql_replace_param(read_where, {'batch_dt': batch_dt})
    if pre_sql:
        pre_sql = parse_sql_replace_param(pre_sql, {'batch_dt': batch_dt})
    # logger.info("开始同步el_type %s 处理表： %s" % (el_type, write_tb))
    logger.debug("%s 导入数据前执行：%s" % (write_tb, pre_sql))
    logger.debug("%s 导入数据条件：%s" % (write_tb, read_where))
    if el_type == 'datax':
        rs = datax(read_tb, read_conn, write_tb, write_conn, read_where, pre_sql, parallel_num, check_tb_strct,
                   logs_print)
    else:
        rs = pypd(read_tb, read_conn, write_tb, write_conn, read_where, pre_sql, check_tb_strct)
    return rs
    # except Exception as e:
    #     err_msg = str(e)
    #     logger.error("同步错误 el_type %s 处理表： %s ERROR: %s" % (el_type, write_tb, err_msg))
    #     raise Exception(err_msg)


def run_el_with_batch(batch_id, el_type, read_tb, read_conn, write_tb, write_conn='DPS', read_where='',
                      pre_sql='truncate', parallel_num=2, check_tb_strct=True, logs_print=True):
    """
    同步表数据
    :param batch_id: 批次批次编号
    :param el_type:  datax或者pypd
    :param read_tb: 读取表名
    :param read_conn: 读取的数据库标识 例如DPS CRM PFUND
    :param write_tb: 写入的表名 例如 dw.dim_prod
    :param write_conn: 写入库的库名 例如DPS CRM PFUND
    :param read_where: sql where条件
    :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
    :param parallel_num: 并发的channel 数
    :param check_tb_strct: 是否检验表结构
    :param logs_print 是否打印日志到终端展示 不管是否设置 日志都会存储到datax/log路径下
    :return:
    """
    dw_conn = Conn(write_conn)
    if write_tb is None:
        write_tb = get_targt_tb_nm(read_tb, read_conn)
    stat = get_el_tb_job_stat(batch_id, write_tb, dw_conn)  # 获取作业状态
    if is_runing(write_tb, dw_conn):  # 如果作业在处理则跳过
        logger.info("el_type %s 处理表： %s 正在处理中不再处理" % (el_type, write_tb))
        el_upd_stat(dw_conn, batch_id, write_tb, batch_stat=1, error_msg="正在处理中不再处理")
    if stat != 1:
        # stat==1 表示执行成功了 不再执行
        el_start_stat(dw_conn, batch_id, write_tb)
        try:
            logger.info("开始同步 batch_id %s el_type %s 处理表： %s" % (batch_id, el_type, write_tb))
            # logger.debug("%s 导入数据前执行：%s" % (write_tb, pre_sql))
            # logger.debug("%s 导入数据条件：%s" % (write_tb, read_where))
            rs = run_el(el_type, read_tb, read_conn, write_tb, write_conn, read_where, pre_sql,
                        parallel_num, check_tb_strct, logs_print, batch_dt=batch_id)
            if rs:
                el_upd_stat(dw_conn, batch_id, write_tb, batch_stat=1)
                logger.info("同步成功 batch_id %s el_type %s 处理表： %s" % (batch_id, el_type, write_tb))
            else:
                raise Exception("不知名错误")
        except Exception as e:
            err_msg = str(e)
            logger.error("同步错误 batch_id %s el_type %s 处理表： %s ERROR: %s" % (batch_id, el_type, write_tb, err_msg))
            el_upd_stat(dw_conn, batch_id, write_tb, batch_stat=2, error_msg=err_msg)
            send_error_msg(err_msg, write_tb, if_to_wx=False)
            raise Exception(err_msg)
        finally:
            dw_conn.close()
    else:
        # 该批次下数据已经同步过，不再同步
        el_upd_stat(dw_conn, batch_id, write_tb, batch_stat=1, error_msg="多次执行，执行跳过")
        logger.warning("该批次下数据已经同步过，不再同步。 batch_id %s el_type %s 处理表： %s" % (batch_id, el_type, write_tb))
        dw_conn.close()


def el_tbs(conn, tb_nm, batch_id=None, logs_print=False):
    """
    同步ods数据主程序入口，同步task_infos所有的未删除表
    :param tb_nm: 如果指定则只同步指定表 也可是多个表 例如ods.pf_cust_info,ods.portal_wechat_card
    :param batch_id:批次ID
    :param conn: 数据库连接
    :param logs_print: 是否打印同步的明细日志
    :return:
    """
    if ',' in tb_nm:
        tbs = tb_nm.replace(' ', '').split(",")
        tb_nm = str(tuple(tbs))
    else:
        tb_nm = "('" + tb_nm + "')"
    if conn is None:
        conn = Conn()
    task_sql = """
        select
            src_tb_nm read_tb,
            src_db_cd read_conn,
            tb_nm write_tb,
            trgt_db_cd write_conn,
            sql_where read_where,
            sql_pre_sql pre_sql,
            el_type,
            parallel_num::int parallel_num
         from {tb_task_info} where is_del='N' and tb_nm in {tb_nm}
        """
    task_sql = task_sql.format(tb_nm=tb_nm, tb_task_info=tb_task_info)
    crt_el_log_tb(conn)
    df = conn.select(task_sql)
    if df.shape[0] > 0:
        rses = df.to_dict(orient='record')
        if batch_id:
            for rs in rses:
                rs['batch_id'] = batch_id
                rs['logs_print'] = logs_print
                run_el_with_batch(**rs)
        else:
            for rs in rses:
                rs['logs_print'] = logs_print
                run_el(**rs)
    else:
        logger.info("没有表需要同步")


def multi_proc_el_all_task(batch_id, conn=None, batch_nm='T1', processes=5, logs_print=False, only_tables=None):
    """
    同步ods数据主程序入口，同步task_infos所有的未删除表
    :param only_tables: 如果指定则只同步指定表
    :param batch_id:批次ID 字符串
    :param conn: 数据库连接
    :param batch_nm: 是否只处理增量表，用于准实时数据同步
    :param processes: 处理进程数个数，默认是5个，根据线上配置个数5-20个
    :param logs_print: 是否打印同步的明细日志
    :return:
    """
    batch_id = str(batch_id)
    if conn is None:
        conn = Conn()
    task_sql = """
        select
            src_tb_nm read_tb,
            src_db_cd read_conn,
            tb_nm write_tb,
            trgt_db_cd write_conn,
            sql_where read_where,
            sql_pre_sql pre_sql,
            el_type,
            parallel_num::int parallel_num
         from {tb_task_info} where is_del='N' and batch_nm='{batch_nm}'
        """
    # is_inc_where = " and batch_nm='{batch_nm}'".format(batch_nm=batch_nm, tb_task_info=tb_task_info)
    task_sql = task_sql.format(batch_nm=batch_nm, tb_task_info=tb_task_info)  # + is_inc_where
    crt_el_log_tb(conn)
    df = conn.select(task_sql)  # pd.read_sql(task_sql, conn)
    if only_tables:  # 如果指定执行某个表
        only_tables = only_tables.replace("，", ",").replace(" ", "").split(",")
        df = df[df['write_tb'].isin(only_tables)]
    if df.shape[0] > 0:
        rs = df.to_dict(orient='record')
        logger.info("Batch_id:%s 启动进程数: %s，开始批处理run_el_with_batch" % (batch_id, processes))
        pool = multiprocessing.Pool(processes=processes)
        for i in rs:
            i['batch_id'] = batch_id
            i['logs_print'] = logs_print
            pool.apply_async(run_el_with_batch, kwds=i)
        pool.close()
        pool.join()
        logger.info("batch_id:%s 完成数据导入" % batch_id)
        if only_tables:
            check_el_task(batch_id, conn, batch_nm, check_error_only=True)
            # 检查作业处理结果，有错误则抛出异常。可以加邮件通知和微信通知
        else:
            check_el_task(batch_id, conn, batch_nm, check_error_only=False, task_list=[i['write_tb'] for i in rs])
            # 检查作业处理结果，有错误则抛出异常。
    else:
        logger.info("没有表需要同步")


def check_el_task(batch_id, conn, batch_nm, check_error_only=False, task_list=None):
    """
    检查数据表同步情况
    :param task_list: 任务执行列表
    :param check_error_only:  是否只检查执行错误的作业，如果是则不判断是否有作业没有被执行
    :param batch_nm: Y或N 检查T+0(Y)或是T+1（N）的
    :param batch_id: 批次ID
    :param conn: 数据库连接
    :return:
    """
    df = get_error_job(batch_id, conn, batch_nm, check_error_only)
    if df is not None and df.shape[0] > 0 and task_list:
        df = df[df['tb_nm'].isin(task_list)]
    if df is not None and df.shape[0] > 0:
        js = df[['tb_nm', 'error_msg']].to_dict(orient='records')
        errors = {"数据处理错误": js}
        errors = json.dumps(errors, ensure_ascii=False)
        logger.error(errors)
        send_error_msg(errors, if_to_wx=True)
        raise Exception(errors)


def run_etl(conn, tb_nm, params=None, action='dml', check_tb_strct=True):
    """
    执行etl文件 或者python 函数
    :param check_tb_strct: 是否强制检测表结构
    :param action: 可以取ddl/dml/func ddl表定义sql dml; 数据操作类sql func 只对应python文件中的函数名称
    :param params: sql 函数参数或者是sql里面的变量 必须是数据字典类型，eg:{'batch_dt','2019-12-10'} 参数为'batch_dt'
    :param tb_nm: 执行的表名或者文件名，规范一个表名对应一个sql文件，文件名称和表名称一致
    :param conn:  数据库连接
    :return:
    """
    logger.info("开始处理表 %s" % (tb_nm,))
    if params and 'check_tb_strct' in params:
        check_tb_strct = params['check_tb_strct']
    if check_tb_strct in [True, 'True', 'true'] and action not in ['el', 'sql']:
        # 如果操作是同步表表不进行表结构校验，因为同步时会校验 el和sql操作不校验表结构
        to_check_tb_strct(None, None, tb_nm, conn, "not raise")
    if action in ('ddl', 'dml', 'sql'):
        sql = get_sql(tb_nm, sql_type=action, if_sql_file_not_exists='raise')
        try:
            if sql:  # 有对应sql文件时
                if action in ('dml', 'sql'):
                    sql = parse_sql_replace_param(sql, params)  # 替换其中的变量
                    logger.debug("提交sql到数据库执行")
                    conn.exec(sql, if_split_to_run=True)
                    logger.debug("表%s的sql处理完成" % tb_nm)
                    return True
                else:
                    if not check_tb_strct:  # 前面没有校验表结构，这里ddl处理需要校验
                        to_check_tb_strct(None, None, tb_nm, conn, "raise")
                    logger.warning("只校验表结构,校验完成")
                    return True
            else:
                raise Exception("没有对应的sql文件，请重新确认路径和文件名")
        except Exception as e:
            # send_error_msg(str(e), tb_nm) 在batch上执行错误提醒
            raise e
    elif action == 'el':  # 数据同步作业
        try:
            logger.info("run_etl同步指定表 ：%s" % (tb_nm,))
            el_tbs(conn, tb_nm)
            return True
        except Exception as e:
            # send_error_msg(str(e), tb_nm)
            raise Exception(str(e))
    else:  # 从python模块中获取执行模块
        try:
            logger.info("执行指定python文件的函数，模块路径：%s/%s 函数名：%s" % (get_base_module(), tb_nm.replace(".", "/"), action))
            script = importlib.import_module(get_base_module() + "." + tb_nm)
        except Exception as e:
            # logger.error(str(e))  # 引入的库不存在
            # send_error_msg(str(e), tb_nm)
            raise Exception(str(e))
        if params is None:
            params = {}
        in_params = params.copy()
        err_msg = "python文件%s没有找到可以执行的函数，请检查执行命令是否错误" % tb_nm
        try:
            all_funs_or_vars = dir(script)
            if action in all_funs_or_vars:  # 函数名称存在
                func = eval("script." + action)
                script_args = list(inspect.signature(func).parameters.keys())  # 获取脚本所需要的参数
                if len(script_args) == 0 or len(params) < 1:
                    func()
                else:
                    for i in in_params.keys():
                        if i not in script_args:
                            params.pop(i)
                    func(**params)
                return True
            else:
                # logger.error(err_msg)
                raise Exception(err_msg)
        except Exception as e:
            # send_error_msg(str(e), tb_nm)
            raise e


def run_etl_with_batch(conn, tb_nm, params, action='dml', check_tb_strct=True, logfile_split=True):
    """
    :param logfile_split: 是否按作业拆分日志文件
    :param check_tb_strct: 是否强制检测表结构
    :param action:  可以取ddl/dml/func ddl表定义sql dml; 数据操作类sql func 只对应python文件中的函数名称
    :param params: sql 文件上参数 必须是数据字典类型
    :param tb_nm: 执行的表名或者文件名，规范一个表名对应一个sql文件，文件名称和表名称一致
    :param conn:  数据库连接
    :return:
    """
    tb_nm = tb_nm.strip()
    if logfile_split:
        fh = get_handler(tb_nm)  # 输出到特定的日志文件
        logger.addHandler(fh)
    if params and 'batch_id' in params:
        batch_id = str(params['batch_id'])
    else:
        raise Exception("batch_id 必须在param参数中给定")
    stat = get_el_tb_job_stat(batch_id, tb_nm, conn)  # 初始化作业统计信息
    if is_runing(tb_nm, conn):  # 如果作业在处理则跳过
        logger.info("sql文件 %s 正在处理中不再处理" % (tb_nm,))
        el_upd_stat(conn, batch_id, tb_nm, batch_stat=1, error_msg="正在处理中不再处理")
    if stat != 1:
        # stat==1 表示执行成功了 不再执行
        logger.info("开始处理 batch_id %s sql文件 %s " % (batch_id, tb_nm))
        el_start_stat(conn, batch_id, tb_nm)
        try:
            rs = run_etl(conn, tb_nm, params, action, check_tb_strct)
            if rs and rs is True:
                el_upd_stat(conn, batch_id, tb_nm, batch_stat=1)
                logger.info("处理成功 batch_id %s  %s 执行成功 " % (batch_id, tb_nm))
            else:
                # logger.error("处理错误 batch_id %s %s sql文件执行失败 " % (batch_id, tb_nm))
                # el_upd_stat(conn, batch_id, tb_nm, batch_stat=2)
                raise Exception("run_etl执行出现不知名错误")
        except Exception as e:
            error_msg = str(e)
            logger.error("处理错误 batch_id %s  %s 执行失败 ERROR: %s" % (batch_id, tb_nm, error_msg))
            el_upd_stat(conn, batch_id, tb_nm, batch_stat=2, error_msg=error_msg)
            send_error_msg(error_msg, tb_nm)
            raise Exception(error_msg)
    else:
        # 该批次下数据已经同步过，不再同步
        el_upd_stat(conn, batch_id, tb_nm, batch_stat=1, error_msg="多次执行，执行跳过")
        logger.info("该批次下数据已经同步过，不再同步。 batch_id %s sql文件 %s " % (batch_id, tb_nm))


def get_targt_tb_nm(tb_nm, db_cd, schema='ods'):
    """
    返回自动生成表结构是的指定的表名称
    :param tb_nm: 可以是无schema的表名也可以是带schema的表名
    :param db_cd:
    :param schema:
    :return:
    """
    if "." in tb_nm:
        tb_nm = tb_nm.split(".")[1]
    perf_db = db_cd.lower() + '_'
    return schema + "." + perf_db + tb_nm


def src_tb_sync_ods(src_tb_nm, src_db_cd, trgt_tb_nm=None, trgt_db_cd=default_db_cd, trgt_schm_nm='ods',
                    if_el_data=True):
    """
    源系统和目标表结构创建或者校验
    :param if_el_data:  是否导入数据
    :param src_tb_nm:
    :param src_db_cd:
    :param trgt_tb_nm:
    :param trgt_db_cd:
    :param trgt_schm_nm:
    :return:
    """
    src_conn = Conn(src_db_cd)
    src_meta = src_conn.get_tb_strct(src_tb_nm)  # 获取表结构
    src_conn.close()
    if src_meta:
        if trgt_tb_nm is None:  # 如果没有设定目标表名，需要自动生成目标表名
            trgt_tb_nm = get_targt_tb_nm(src_meta['tb_nm'], src_meta['db_cd'], schema=trgt_schm_nm)
        crt_tb_sql = crt_trgt_db_sql(src_meta, trgt_tb_nm, trgt_db_cd)
        trgt_conn = Conn(trgt_db_cd)
        rs = trgt_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
        trgt_conn.close()
        if if_el_data:
            datax(src_tb_nm, src_db_cd, trgt_tb_nm, write_conn=trgt_db_cd, check_tb_strct=False, logs_print=False)
        return rs
    else:
        raise Exception("源数据库目标表表%s不存在" % src_tb_nm)


def _el_run(batch_id, el_type, src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd=default_db_cd, read_where='',
            pre_sql='truncate',
            parallel_num=2, if_el_data=False):
    """
        用于按系统的schema全量同步表,分情况调用el_run和datax，仅用于 multi_proc_el
        :param batch_id: 批次批次编号
        :param el_type:  datax或者pypd
        :param src_tb_nm: 读取表名
        :param src_tb_nm: 读取的数据库标识 例如DPS CRM PFUND
        :param trgt_tb_nm: 写入的表名 例如 dw.dim_prod
        :param trgt_db_cd: 写入库的库名 例如DPS CRM PFUND
        :param read_where: sql where条件
        :param pre_sql:  导入前sql操作，truncate 表示清空表，可以有其他sql
        :param parallel_num: 并发的channel 数
        :return:
    """
    to_check_tb_strct(src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd, if_ddl_not_exists="raise")  # 校验表结构，存在则检查变化，不存在创建
    if if_el_data:  # 判断是否导入数据
        if batch_id:  # 是否指定了batch_id
            run_el_with_batch(batch_id, el_type, src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd, read_where, pre_sql,
                              parallel_num, False, False)
        else:
            datax(src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd, read_where, pre_sql, parallel_num, False, False)
    else:
        logger.warning("数据不导入")


def multi_proc_el(df, batch_id, processes=5, trgt_schm_nm='ods'):
    """
    对df数据多进程处理，这里是导入数据或建表格
    :param batch_id:
    :param df: dataframe 需要字段batch_id, el_type, src_tb_nm, src_db_cd, trgt_tb_nm,
                         trgt_db_cd=default_db_cd, read_where='', pre_sql='truncate', parallel_num=2
    :param processes:  多进程数
    :param trgt_schm_nm:  目标schema 主要用于
    :return:
    """
    rs = df.to_dict(orient='record')
    logger.info("批量导入 启动进程数: %s，开始批处理" % (processes,))
    pool = multiprocessing.Pool(processes=processes)
    # i 需要传递字段 batch_id, el_type, src_tb_nm, src_db_cd, trgt_tb_nm, trgt_db_cd,
    # read_where='', pre_sql='truncate', parallel_num
    for i in rs:
        i['batch_id'] = batch_id
        if i['trgt_tb_nm'] is None:
            i['trgt_tb_nm'] = get_targt_tb_nm(i['src_tb_nm'], i['src_db_cd'], schema=trgt_schm_nm)
        # logger.info(str(i))
        pool.apply_async(_el_run, kwds=i)
    pool.close()
    pool.join()
    logger.info("完成批处理")
    conn = Conn()
    check_el_task(batch_id, conn, batch_nm='T1', check_error_only=True)
    conn.close()


def crt_all_tb_from_src(src_db_cd, src_tb_schm, trgt_db_cd="DPS", trgt_schm_nm='ods', batch_id=None, if_el_data=False):
    """
    一次性移动某个系统的表到数据仓库中，一般只有临时性导入某数据库才这样操作,不能用于生产调度
    :param if_el_data:  是导入源系统数据。默认只建表结构
    :param batch_id:   有batch_id的,run_el_with_batch 导入并记录到task_log里面，没有指定的直接导datax导数据
    :param src_db_cd: 源系统编号
    :param src_tb_schm: 源系统schema 只需要指定shcema 没有的用public
    :param trgt_db_cd: 目标系统编号 DPS HQ_HR
    :param trgt_schm_nm: 目标系统schema
    :return:
    """
    conn = Conn(src_db_cd)
    # conn, db_type, db_nm = get_db_conn(src_db_cd)
    if conn.db_type.upper() == 'ORACLE'.upper():
        sql = """select distinct table_name tb_nm from user_tables  order by  table_name"""
    elif conn.db_type.upper() == 'MYSQL'.upper():
        sql = """
           select distinct t2.table_name tb_nm
           from   information_schema.tables t2 
           where t2.table_schema='{db_nm}' and table_type='BASE TABLE';""".format(db_nm=conn.db_nm)
    elif conn.db_type.upper() == 'PostgreSQL'.upper():
        sql = """ 
           SELECT   distinct
                          '{tb_schm}.'||B.relname tb_nm  
                       FROM  pg_catalog.pg_class B
                       INNER JOIN pg_catalog.pg_namespace C
                           ON C.oid=B.relnamespace
                           AND C.nspname='{tb_schm}' where B.relam=0  and relkind='r' 
           """.format(tb_schm=src_tb_schm)
    elif conn.db_type.upper() == 'Greenplum'.upper():
        sql = """ 
           SELECT   distinct
                          '{tb_schm}.'||B.relname tb_nm  
                       FROM  pg_catalog.pg_class B
                       INNER JOIN pg_catalog.pg_namespace C
                           ON C.oid=B.relnamespace
                           AND C.nspname='{tb_schm}' where B.relam=0  and relkind='r' 
           """.format(tb_schm=src_tb_schm)
    elif conn.db_type.upper() == 'SQLServer'.upper():
        sql = """SELECT
                  distinct sysobjects.name AS tb_nm 
               FROM syscolumns                            -- 数据表字段
               INNER JOIN sysobjects                        -- 数据对象
                 ON sysobjects.id = syscolumns.id
               INNER JOIN systypes                         -- 数据类型
                 ON syscolumns.xtype = systypes.xtype
               LEFT OUTER JOIN sys.extended_properties properties       -- 字段属性信息
                 ON syscolumns.id = properties.major_id
                AND syscolumns.colid = properties.minor_id
               LEFT OUTER JOIN sys.extended_properties sysproperties                -- 表属性信息
                 ON sysobjects.id = sysproperties.major_id
                AND sysproperties.minor_id = 0
               LEFT OUTER JOIN syscomments                -- 注释信息
                 ON syscolumns.cdefault = syscomments.id
               LEFT OUTER JOIN sysindexkeys                -- 索引中的键或列的信息
                 ON sysindexkeys.id = syscolumns.id
                AND sysindexkeys.colid = syscolumns.colid
               LEFT OUTER JOIN sysindexes                  -- 数据库 索引表
                 ON sysindexes.id = sysindexkeys.id
                AND sysindexes.indid = sysindexkeys.indid
               LEFT OUTER JOIN sysforeignkeys
                 ON sysforeignkeys.fkeyid = syscolumns.id
                AND sysforeignkeys.fkey = syscolumns.colid
               left join INFORMATION_SCHEMA.tables ts on sysobjects.name =ts.table_name
               WHERE (sysobjects.xtype = 'U') and ts.Table_catalog='{db_nm}' 
               order by syscolumns.id""".format(db_nm=conn.db_nm, )
    else:
        # logger.error("不支持的数据库类型：" + db_type)
        raise Exception("不支持的数据库类型：" + conn.db_type)
    # logger.warning(sql)
    meta = conn.select(sql)  # pd.read_sql(sql, conn)
    if meta.shape[0] > 0:
        meta.columns = ['src_tb_nm']  # (map(lambda x: x.lower(), list(meta.columns)))
        meta['el_type'] = 'datax'
        meta['src_db_cd'] = src_db_cd
        meta['trgt_tb_nm'] = None
        meta['trgt_db_cd'] = trgt_db_cd
        meta['read_where'] = ''
        meta['pre_sql'] = 'truncate'
        meta['parallel_num'] = 2
        meta['if_el_data'] = if_el_data
        multi_proc_el(meta, batch_id, processes=5, trgt_schm_nm=trgt_schm_nm)
    logger.info("数据处理完成")
