# cython: language_level=3
import importlib
import multiprocessing
import subprocess
import time
import os
import re
import inspect
from etls.comm.datefunc import Date as BatchDate
from etls.comm.batch import BatchParams
from etls.comm.emailcomm import send_error_msg
from etls.conf.settings import etl_root_module, tb_task_info, tb_task_log
from etls.comm.conn import Conn, extract_table_name_from_sql
# from etl.comm.loggers import get_logger
from etls.comm.dataxy import crt_trgt_db_sql, datax, pypd, check_el_task, path_ddl_sql, path_dml_sql
import logging

logger = logging.getLogger()

# datax_path = os.path.join(BASE_DIR, 'datax/bin/datax.py')
# tb_task_info = "public.task_infos"  # 同步配置表源表-目标表
# tb_task_log = "public.task_logs"  # 同步日志表表名
# path_ddl_sql = path_of_ddl_sql  # ddl表定义sql路径
# path_dml_sql = path_of_dml_sql  # dml表执行sql路径
jobtype = {
    'batch_el': 'batch_el',  # 批量表数据同步
    'el': 'el',  # 单表同步
    'sql': 'sql',  # 执行sql 不校验表结构
    'dml': 'dml',  # dml 语句 校验表结构
    'ddl': 'ddl',  # 只校验表结构
    'py': 'py',  # python脚本 不指定job type时已python脚本执行
}


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
    解析sql语句中的变量参数，返回参数列表
    :param sql:
    :return:
    """
    if sql and len(sql) > 4:
        p1 = re.compile(r'[{](\S*)[}]')  # 最小匹配 \S 任意非空字符 \w排除了空格 匹配字母数字及下划线，即a-z、A-Z、0-9、_
        params = re.findall(p1, sql)
        return list(set(params))
    else:
        return []


def parse_sys_args(args):
    if isinstance(args, str):
        args = args.replace("，", ",").split()
    schm_tb_nm = args[1].replace("，", ",")
    job_type = args[2]  # job_type 必要参数 可以取ddl/dml/func/el/batch_el
    param_list = args[3:]
    param_list_len = len(param_list)
    if param_list_len > 0:  # 有代入参数
        if param_list_len % 2 == 0:
            keys = [param_list[i].replace("-", "") for i in range(param_list_len) if i % 2 == 0]
            vals = [param_list[i] for i in range(param_list_len) if i % 2 == 1]
            params_dict = dict(zip(keys, vals))
        else:
            err_msgs = " ".join(param_list) + " 参数应该要kv一一对应"
            logger.error(err_msgs)
            raise Exception(err_msgs)
    else:
        params_dict = {}
    if 'conn' in params_dict:
        conn = Conn(params_dict['conn'])
    else:
        conn = Conn()
    if 'p' in params_dict:  # 进程池进程个数
        params_dict['processes'] = params_dict['p']
    if 'c' in params_dict:  # 是否检查表结构
        params_dict['check_tb_strct'] = params_dict['c']
    if 'nm' in params_dict:
        params_dict['batch_nm'] = params_dict['nm']
    return conn, schm_tb_nm, job_type, params_dict


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


def exec_shell(cmd, log_stdout=False, timeout=1000):
    # try:
    #     proc = subprocess.Popen(shell, shell=True, stdout=subprocess.PIPE)
    #     while proc.poll() is None:
    #         line = proc.stdout.readline().strip().decode('utf-8')
    #         if logs_print:
    #             logger.info(line)
    #     error=proc.stderr.read().strip().decode('utf-8')
    # except Exception as e:
    #     logger.info('shell执行失败：' + str(e))
    #     raise Exception("shell执行失败:" + str(e))
    # finally:
    #     return proc.returncode, error
    data = {"rs": False, "timeout": False, "stdout": "", "stderr": ""}
    try:
        if log_stdout:
            stdout = subprocess.PIPE
        else:
            stdout = None
        logger.info("执行cmd:%s " % cmd)
        if os.name == 'nt':  # nt 表示Windwos系操作系统， posix 代表类Unix或OS X系统。
            process = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=subprocess.PIPE)
        else:
            process = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
        try:
            outs, errs = process.communicate(timeout=timeout)
            data["stdout"] = outs.decode("utf-8")
            data["stderr"] = errs.decode("utf-8")
            data["rs"] = True
            data['returncode'] = process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            outs, errs = process.communicate()
            data['returncode'] = 99
            data["rs"] = False
            data["stdout"] = outs.decode("utf-8")
            data["stderr"] = "timeout"
            data["timeout"] = True
    except Exception as e:
        data['returncode'] = 99
        data["rs"] = False
        data["stderr"] = str(e)
    finally:
        return data


def exec_job(conn, tb_nm, job_type, job_params=None, batch_id=None, check_tb_strct=True, logs_print=False):
    job = Job(conn, tb_nm, job_type, job_params, batch_id, check_tb_strct, logs_print)
    job.exec()


def get_bool_val(col):
    if col is None:
        return False
    if isinstance(col, bool):
        return col
    else:
        if str(col).lower() == 'true':
            return True
        else:
            return False


class Job(object):

    def __init__(self, conn, tb_nm, job_type, job_params=None, batch_id=None, check_tb_strct=True, logs_print=True):
        # 表名称 包含schema str
        self.tb_nm = tb_nm.strip()
        self.conn = conn if isinstance(conn, Conn) else Conn(conn)
        self.job_params = job_params if job_params else {}
        self.batch_id = batch_id if batch_id else self.job_params.get('batch_id', None)
        # 作业类型 ddl dml el py sql batch_el, 如果是python作业，则job_type是python文件中对应的函数名
        if job_type.lower() in list(jobtype.values()):
            self.job_type = job_type.lower()
            self.action = self.job_params.get('el_type', 'datax').lower()
            if job_type.lower() == jobtype['el'] and self.tb_nm.lower() == jobtype['batch_el']:
                self.job_type = 'batch_el'
                if 'batch_nm' in self.job_params:
                    self.tb_nm = self.job_params.get('batch_nm')
                else:
                    # 读取对应批次ID的参数文件
                    paramsfile = BatchParams(self.batch_id).read()
                    if 'batch_nm' in paramsfile:
                        self.tb_nm = paramsfile.get('batch_nm')
                    else:
                        raise Exception("batch_el 批量导入作业类型必须指定batch_nm")
        else:
            self.job_type = 'py'
            self.action = job_type
        self.check_tb_strct = get_bool_val(
            check_tb_strct if check_tb_strct is not None else self.job_params.get('check_tb_strct', True))
        self.logs_print = get_bool_val(
            logs_print if logs_print is not None else self.job_params.get('logs_print', True))

    def is_running(self) -> bool:
        """作业是否在执行"""
        conn = self.conn
        # crt_el_log_tb(self.conn)  # 检查日志表的变化，todo 后面看看优化
        sql = """select batch_id from {log_tb_nm} where tb_nm='{tb_nm}' 
                    and start_dt>= current_timestamp-interval '3 hour' and batch_stat=0"""
        sql = sql.format(log_tb_nm=tb_task_log, tb_nm=self.tb_nm)
        flag = conn.query_one(sql)
        times = 0
        while flag:
            logger.warning("%s在执行中本次执行开始等待，等待第%s次" % (self.tb_nm, times))
            send_error_msg("batch_id:%s的作业%s一直在执行中本批次%s等待，请确认是否有执行故障" % (flag, self.tb_nm, self.batch_id), self.tb_nm,
                           if_to_wx=False)
            time.sleep(300)  # 等待5分钟
            times += 1
            flag = conn.query_one(sql)
            if flag and times > 3:
                logger.error("%s一直在执行中，请确认是否产生故障。此次执行报错" % self.tb_nm)
                raise Exception("%s一直在执行中，请确认是否产生故障。此次执行报错" % self.tb_nm)
        return True

    def get_exec_file(self, if_sql_file_not_exists="not raise"):
        """
        读取执行文件的内容
        :param if_sql_file_not_exists:
        :return:
        """
        if self.job_type.lower() == jobtype['ddl']:
            return self.get_ddl_sql(if_sql_file_not_exists)
        elif self.job_type.lower() == jobtype['dml']:
            return self.get_dml_sql(if_sql_file_not_exists)
        elif self.job_type.lower() == jobtype['py']:
            py_file = os.path.join(path_dml_sql, self.tb_nm.replace(".", "/")) + ".py"
            if not os.path.exists(py_file):
                if if_sql_file_not_exists == 'raise':
                    raise Exception("%s 文件不存在" % py_file)
                return None
            else:
                with open(py_file, 'r') as f:
                    py_txt = f.read()
                return py_txt
        else:
            if if_sql_file_not_exists == 'raise':
                raise Exception("%s该作业类型不支持读取内容文件")
            return None

    def get_ddl_sql(self, if_sql_file_not_exists='not raise'):
        """
            获取ddl
            :rtype: str
            :param if_sql_file_not_exists: 如果sql文件不存在是否强制抛出异常，默认不抛出
            :return:
        """
        # 按照schema目录的的方式存储，ddl/dw/dim_date.sql
        sql_file = os.path.join(path_ddl_sql, self.tb_nm.replace(".", "/")) + ".sql"
        if not os.path.exists(sql_file):
            # 如果ddl/dw/dim_date.sql 找不到 则按照dml/dw.dim_date.sql查找
            sql_file = os.path.join(path_ddl_sql, self.tb_nm) + ".sql"
        if os.path.exists(sql_file):
            logger.debug("获取sql文件：%s" % (sql_file,))
            sql_file = open(sql_file, 'r')
            sql = sql_file.read()
            sql_file.close()
            return sql.strip()
        else:
            if self.job_type.lower() == jobtype['ddl'] and os.path.exists(self.tb_nm):  # 作为sql文件路径执行
                logger.debug("获取sql文件：%s" % (self.tb_nm,))
                sql_file = open(self.tb_nm, 'r')
                sql = sql_file.read()
                sql_file.close()
                return sql.strip()
            else:
                logger.warning("sql文件不存在：%s " % (sql_file,))
                if if_sql_file_not_exists == 'raise':
                    logger.error("sql文件不存在：%s " % (sql_file,))
                    raise Exception("sql文件不存在：%s " % (sql_file,))

    def get_dml_sql(self, if_sql_file_not_exists='not raise'):
        """
            获取sql文件下的sql语句
            :param if_sql_file_not_exists: 如果sql文件不存在是否强制抛出异常，默认不抛出
            :return:
        """
        # 按照schema目录的的方式存储，dml/dw/dim_date.sql
        sql_file = os.path.join(path_dml_sql, self.tb_nm.replace(".", "/")) + ".sql"
        if not os.path.exists(sql_file):
            # 如果dml/dw/dim_date.sql 找不到 则按照dml/dw.dim_date.sql查找
            sql_file = os.path.join(path_dml_sql, self.tb_nm) + ".sql"
        if os.path.exists(sql_file):
            logger.debug("获取sql文件：%s" % (sql_file,))
            sql_file = open(sql_file, 'r')
            sql = sql_file.read()
            sql_file.close()
            return sql.strip()
        else:
            if os.path.exists(self.tb_nm):  # 作为sql文件路径执行
                logger.debug("获取sql文件：%s" % (self.tb_nm,))
                sql_file = open(self.tb_nm, 'r')
                sql = sql_file.read()
                sql_file.close()
                return sql.strip()
            elif os.path.exists(os.path.join(path_dml_sql, self.tb_nm)):  # 作为sql文件路径执行
                tb_nm = os.path.join(path_dml_sql, self.tb_nm)
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

    def set_start_stat(self):
        """
        任务开始状态
        :return:
        """
        if self.batch_id:
            stat = self.get_job_stat()
            if stat < 0:
                batch_stat = 0  # 0 表示任务开始
                sql = """insert into {log_tb_nm} (batch_id,tb_nm,batch_stat,start_dt) values('{batch_id}','{tb_nm}',
                        {batch_stat},current_timestamp)"""
                sql = sql.format(log_tb_nm=tb_task_log, batch_id=self.batch_id, tb_nm=self.tb_nm, batch_stat=batch_stat)
                self.conn.exec(sql, if_print_sql=False)
        else:
            logger.warning("batch_id为空，不记录数据库中")

    def upd_job_stat(self, batch_stat, error_msg=''):
        """
            更新el作业状态
            :param batch_stat:    0 表示任务开始; 1 表示完成 成功 ;2 表示 失败 完成
            :param error_msg:  错误信息
            :return:
        """
        if self.batch_id:
            if len(error_msg) > 2:
                sql = """update {log_tb_nm} set batch_stat= %s,end_dt=current_timestamp,error_msg= %s
                            where  tb_nm= %s and batch_id= %s ;"""
                param = (batch_stat, error_msg, self.tb_nm, str(self.batch_id))
            else:
                sql = """update {log_tb_nm} set batch_stat= %s,end_dt=current_timestamp
                                    where  tb_nm= %s and batch_id= %s ;"""
                param = (batch_stat, self.tb_nm, str(self.batch_id))
            sql = sql.format(log_tb_nm=tb_task_log)
            self.conn.exec(sql, param, if_print_sql=False)
        else:
            logger.warning("batch_id为空，不记录数据库中")

    def get_job_stat(self):
        if self.batch_id:
            sql = """select batch_id,tb_nm,batch_stat stat from {log_tb_nm} 
                         where batch_id='{batch_id}' and tb_nm='{tb_nm}' order by start_dt desc limit 1"""
            sql = sql.format(batch_id=self.batch_id, log_tb_nm=tb_task_log, tb_nm=self.tb_nm)
            df = self.conn.select(sql)
            if df.shape[0] > 0:
                return df.loc[0, 'stat']
            else:
                return -1
        else:
            logger.warning("batch_id为空，无状态可查")
            return -1

    def _exec(self):
        """
        不考虑批次的情况下执行，一般不能外部调用，外部调用执行exec即可
        :return:
        """
        logger.info("开始处理表%s: %s check_table_struct %s " % (self.conn.db_cd, self.tb_nm, self.check_tb_strct))

        if self.check_tb_strct and self.job_type != 'ddl':
            # 如果操作是同步表表不进行表结构校验，因为同步时会校验
            self.check_table_struct()
        if self.job_type.lower() == jobtype['ddl']:
            self.check_table_struct()
            logger.warning("只校验表结构,校验完成")
            return True
        elif self.job_type in ['dml', 'sql']:
            sql = self.get_dml_sql(if_sql_file_not_exists='raise')
            if sql:  # 有对应sql文件时
                sql = parse_sql_replace_param(sql, self.job_params)  # 替换其中的变量
                logger.debug("提交sql到数据库执行")
                self.conn.exec(sql, if_split_to_run=True)
                logger.debug("表%s的sql处理完成" % self.tb_nm)
                return True
            else:
                raise Exception("没有对应的sql文件，请重新确认路径和文件名")
        elif self.job_type == jobtype['el']:  # 数据同步作业
            logger.info("el作业,同步指定表 ：%s" % (self.tb_nm,))
            if self.check_tb_strct is False:
                self.get_el_info()
            self._el()
            return True
        elif self.job_type == jobtype['batch_el']:  # 数据同步作业
            logger.info("batch_el作业,批量同步 ：%s" % (self.tb_nm,))
            self._batch_el()
            return True
        elif self.job_type == jobtype['py']:  # 数据同步作业 # 从python模块中获取执行模块
            fun = self.action  # 对应的函数
            logger.info(
                "执行指定python文件的函数，模块路径：%s/%s 函数名：%s" % (get_base_module(), self.tb_nm.replace(".", "/"), fun))
            script = importlib.import_module(get_base_module() + "." + self.tb_nm)
            in_params = self.job_params.copy()
            err_msg = "python文件%s没有找到可以执行的函数，请检查执行命令是否错误" % self.tb_nm
            all_funs_or_vars = dir(script)
            if fun in all_funs_or_vars:  # 函数名称存在
                func = eval("script." + fun)
                script_args = list(inspect.signature(func).parameters.keys())  # 获取脚本所需要的参数
                params = self.job_params.copy()
                if len(script_args) == 0 or len(params) < 1:
                    func()
                else:
                    for i in in_params.keys():
                        if i not in script_args:
                            params.pop(i)
                    func(**params)
                return True
            else:
                raise Exception(err_msg)
        else:
            raise Exception("不支持的作业类型")

    def get_dependence(self):
        """
            自动获取获取作业依赖
            :return:
        """
        sql = self.get_exec_file(if_sql_file_not_exists='raise')
        tb = extract_table_name_from_sql(sql)
        depend = [i for i in tb if len(i) > 4 and not i.startswith('tp') and not i.startswith('temp')]
        return depend

    def check_dependence_stat(self):
        pass

    def check_table_struct(self, if_ddl_not_exists="raise"):
        """
        校验表结构
        :param if_ddl_not_exists:
        :return:
        """
        if self.job_type in ['sql', 'batch_el'] or self.tb_nm.lower() == 'batch_el':
            logger.warning("不需要校验表结构")
            return
        sql = self.get_ddl_sql()  # 获取表结构的sql文件
        trgt_tb_nm = self.tb_nm
        rses = self.get_el_info()
        if sql:  # 存在sql文件时，以sql文件为主
            conn = self.conn
            check_rs = conn.upd_tb_strct(sql, trgt_tb_nm)
            if check_rs is False:
                raise Exception("表结构校验不通过，请查看上面的错误明细")
        elif self.job_type == 'el':
            src_tb_nm = rses.get('src_tb_nm')
            src_db_cd = rses.get('src_db_cd')
            err_msg = "根目录下的ddl/" + trgt_tb_nm.replace(".", "/") + ".sql文件不存在，从源系统拉取表结构校验"
            logger.warning(err_msg)
            if src_tb_nm and src_db_cd:
                self.crt_tb_from_src_sys(src_tb_nm, src_db_cd)  # 校验表结构并修改
            else:
                raise Exception("el作业必须包含参数src_tb_nm,src_db_cd")
        else:
            err_msg = "根目录下的ddl/" + trgt_tb_nm.replace(".", "/") + ".sql 文件不存在，无法校验"
            logger.warning(err_msg)
            if if_ddl_not_exists == "raise":
                raise Exception(err_msg)

    def get_auto_el_tb_nm(self, schema='ods'):
        """
        主要用于自动同步表结构，生成表名称。返回自动生成表结构是的指定的表名称
        :param schema:
        :return:
        """
        tb_nm = self.tb_nm
        if "." in tb_nm:
            tb_nm = tb_nm.split(".")[1]
        perf_db = self.conn.db_cd.lower() + '_'
        return schema + "." + perf_db + tb_nm

    def crt_tb_from_src_sys(self, src_tb_nm, src_db_cd, trgt_schm_nm='ods'):
        """
        数据同步时，源系统和目标表结构创建或者校验
        :param src_tb_nm:
        :param src_db_cd:
        :param trgt_schm_nm:
        :return:
        """
        trgt_tb_nm = self.tb_nm
        trgt_db_cd = self.conn.db_cd
        src_conn = Conn(src_db_cd)
        src_meta = src_conn.get_tb_strct(src_tb_nm)  # 获取表结构
        src_conn.close()
        if src_meta:
            if trgt_tb_nm is None:  # 如果没有设定目标表名，需要自动生成目标表名
                trgt_tb_nm = self.get_auto_el_tb_nm(schema=trgt_schm_nm)
            crt_tb_sql = crt_trgt_db_sql(src_meta, trgt_tb_nm, trgt_db_cd)
            trgt_conn = Conn(trgt_db_cd)
            rs = trgt_conn.upd_tb_strct(crt_tb_sql, schm_tb_nm=trgt_tb_nm, drop_direct=False)
            trgt_conn.close()
            return rs
        else:
            raise Exception("源数据库目标表表%s不存在" % src_tb_nm)

    def get_cols(self):
        """
        获取字段名
        :return:
        """
        rs = self.conn.get_tb_strct(self.tb_nm)  # 获取表结构
        if rs:
            rs = rs['meta']['col_nm']
            cols = list(rs)
            return cols  # '"' + '","'.join(cols) + '"'

    def get_el_info(self):
        if self.job_type == jobtype['el'] and 'src_db_cd' not in self.job_params:
            task_sql = """
                            select
                                src_tb_nm ,
                                src_db_cd ,
                                tb_nm ,
                                trgt_db_cd ,
                                sql_where read_where,
                                sql_pre_sql pre_sql,
                                el_type,
                                parallel_num::int parallel_num
                             from {tb_task_info} where is_del='N' and tb_nm = '{tb_nm}'
                            """
            task_sql = task_sql.format(tb_nm=self.tb_nm, tb_task_info=tb_task_info)
            df = self.conn.select(task_sql)
            if df.shape[0] > 0:
                rses = df.to_dict(orient='record')[0]
                self.job_params.update(rses)
            return self.job_params
        else:
            return self.job_params

    def _batch_el(self):
        """
        同步ods数据主程序入口，同步task_infos所有的未删除表志
        :return:
        """
        batch_id = self.batch_id
        batch_nm = self.tb_nm
        if batch_nm is None:
            raise Exception("batch_el 批量导入必须给定batch_nm")
        only_tables = self.job_params.get('only_tables', None)
        processes = int(self.job_params.get('processes', 10))
        task_sql = """
                select
                    src_tb_nm ,
                    src_db_cd ,
                    tb_nm ,
                    trgt_db_cd conn,
                    sql_where read_where,
                    sql_pre_sql pre_sql,
                    el_type,
                    parallel_num::int parallel_num
                 from {tb_task_info} where is_del='N' and batch_nm='{batch_nm}'
                """
        task_sql = task_sql.format(batch_nm=batch_nm, tb_task_info=tb_task_info)  # + is_inc_where
        df = self.conn.select(task_sql)  # pd.read_sql(task_sql, conn)
        if only_tables:  # 如果指定执行某个表
            only_tables = only_tables.replace("，", ",").replace(" ", "").split(",")
            df = df[df['write_tb'].isin(only_tables)]
        if df.shape[0] > 0:
            rs = df.to_dict(orient='record')
            logger.info("Batch_id:%s 启动进程数: %s，开始批处理" % (batch_id, processes))
            pool = multiprocessing.Pool(processes=processes)
            for i in rs:
                tp = {'conn': i['conn'], 'tb_nm': i['tb_nm'], 'job_type': 'el', 'batch_id': batch_id, 'job_params': i}
                pool.apply_async(exec_job, kwds=tp)
            pool.close()
            pool.join()
            logger.info("batch_id:%s 完成数据导入" % batch_id)
            if only_tables:
                check_el_task(batch_id, self.conn, batch_nm, check_error_only=True)
                # 检查作业处理结果，有错误则抛出异常。可以加邮件通知和微信通知
            else:
                check_el_task(batch_id, self.conn, batch_nm, check_error_only=False,
                              task_list=[i['tb_nm'] for i in rs])
                # 检查作业处理结果，有错误则抛出异常。
        else:
            logger.info("没有表需要同步")

    def _el(self):
        """
        数据同步操作
        :return:
        """
        if self.job_type == 'el':
            rses = self.job_params  # get_el_info()
            if rses:
                read_where = rses.get('read_where', None)
                pre_sql = rses.get('pre_sql', None)
                src_tb_nm = rses.get('src_tb_nm', None)
                src_db_cd = rses.get('src_db_cd', None)
                if src_db_cd and src_tb_nm:
                    trgrt_tb_nm = self.tb_nm
                    trgrt_db_cd = self.conn.db_cd
                    parallel_num = rses.get('parallel_num', 2)
                    if read_where:
                        read_where = parse_sql_replace_param(read_where, {'batch_id': self.batch_id})
                    if pre_sql:
                        pre_sql = parse_sql_replace_param(pre_sql, {'batch_id': self.batch_id})
                    logger.debug("%s 导入数据前执行：%s" % (trgrt_tb_nm, pre_sql))
                    logger.debug("%s 导入数据条件：%s" % (trgrt_tb_nm, read_where))
                    if self.action == 'datax':
                        rs = datax(src_tb_nm, src_db_cd, trgrt_tb_nm, trgrt_db_cd, read_where, pre_sql,
                                   parallel_num,
                                   False, self.logs_print)
                    elif self.action == 'pypd':
                        rs = pypd(src_tb_nm, src_db_cd, trgrt_tb_nm, trgrt_db_cd, read_where, pre_sql,
                                  False)
                    else:
                        raise Exception("不支持的同步类型，目前只支持datax和pypd")
                    return rs
                else:
                    raise Exception("%s 同步配置出错,必须给定源系统的编码和源系统的表名" % self.tb_nm)
            else:
                raise Exception("%s 同步配置出错,必须给定源系统的编码和源系统的表名" % self.tb_nm)
        else:
            raise Exception("%s 不是el类型作业" % self.tb_nm)

    def exec(self):
        """
        :return:
        """
        tb_nm = self.tb_nm
        if self.batch_id:  # 有批次ID的，一般定时都要batch_id
            self.is_running()  # 如果作业在处理则跳过
            # logger.info("sql文件 %s 正在处理中不再处理" % (tb_nm,))
            # send_error_msg("正在处理中不再处理,请检查作业是否有问题", tb_nm, if_to_wx=False)
            # self.upd_job_stat(batch_stat=1, error_msg="正在处理中不再处理")
            stat = self.get_job_stat()  # 初始化作业统计信息
            if stat != 1 or self.job_type == jobtype['batch_el']:
                # stat==1 表示执行成功了 不再执行
                logger.info("开始处理 batch_id %s sql文件 %s " % (self.batch_id, tb_nm))
                self.set_start_stat()  # 设置开始作业状态
                try:
                    rs = self._exec()
                    if rs and rs is True:
                        self.upd_job_stat(batch_stat=1)
                        logger.info("处理成功 batch_id %s  %s 执行成功 " % (self.batch_id, tb_nm))
                    else:
                        raise Exception("%s 执行错误" % tb_nm)
                except Exception as e:
                    error_msg = str(e)
                    logger.error("处理错误 batch_id %s  %s 执行失败 ERROR: %s" % (self.batch_id, tb_nm, error_msg))
                    self.upd_job_stat(batch_stat=2, error_msg=error_msg)
                    send_error_msg(error_msg, tb_nm)
                    raise Exception(error_msg)
            else:
                # 该批次下数据已经同步过，不再同步
                self.upd_job_stat(batch_stat=1, error_msg="多次执行，执行跳过")
                logger.info("该批次下数据已经同步过，不再同步。 batch_id %s sql文件 %s " % (self.batch_id, tb_nm))
        else:
            rs = self._exec()
            if rs and rs is True:
                logger.info("%s 执行成功 " % (tb_nm,))
            else:
                raise Exception("%s 执行错误" % tb_nm)
