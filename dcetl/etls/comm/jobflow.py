# cython: language_level=3
import copy
import time
from datetime import datetime
from queue import Queue
from etls.comm.dataxy import crt_el_log_tb
from etls.conf.settings import data_file_path
from etls.comm.conn import Conn
from etls.comm.job import exec_job, tb_task_log, exec_shell, parse_sys_args
from dask.multiprocessing import get  # , config, get_context, initialize_worker_process
from etls.comm.loggers import get_logger
import os
import pandas as pd
import re

logger = get_logger('jobflows')
flows_path = os.path.join(data_file_path, 'flows')


# 以下用于基于依赖关系计算先后顺序3
def sort_by_depend(path):
    # def get_jobs(path='/home/xzh/dps/etl/dpsetl/dpsetl/dw_daily.csv'):
    df = pd.read_csv(path, index_col='job_nm')

    no_dep_jobs = list(df[pd.isna(df['dependencies'])].index)  # .sort()
    # print(no_dep_jobs)
    df.drop(index=no_dep_jobs, inplace=True)
    df['dependencies'] = df['dependencies'].apply(lambda x: str(x).replace("，", ",").strip().split())
    dep = df.to_dict()['dependencies']
    # print(dep['task_infos'])
    sort_jobs = no_dep_jobs.copy()
    while dep:
        tp_list = []
        sort_jobs_set = set(sort_jobs)
        tp_dep = copy.deepcopy(dep)
        for i in tp_dep.keys():
            if set(dep[i]) <= sort_jobs_set:  # 子集判断
                tp_list.append(i)
                dep.pop(i)
        if tp_list:
            print(tp_list)
            tp_list.sort()
            sort_jobs.extend(tp_list)
    print(sort_jobs)


def exec_job_by_depend(*args):
    if args:
        cmd = args[0]
        conn, schm_tb_nm, job_type, params_dict = parse_sys_args(cmd)
        # print('params_dict', params_dict)
        exec_job(conn, schm_tb_nm, job_type, job_params=params_dict,
                 check_tb_strct=params_dict.get('check_tb_strct', True))
        return schm_tb_nm
    else:
        raise Exception("输入不能为空")


# def get_jobs(path='/home/xzh/dps/etl/dpsetl/dpsetl/dw_daily.csv'):
#     df = pd.read_csv(path, index_col='job_nm')
#     df.fillna({'dependencies': 'start'}, inplace=True)
#     df['dependencies'] = df['dependencies'].apply(lambda x: str(x).replace("，", ",").strip().split())
#
#     # df.loc['start', 'command']
#
#     # def exec_job_by_depend(*args):
#     #     if args:
#     #         cmd = args[0]
#     #         conn, schm_tb_nm, job_type, params_dict = parse_params(cmd)
#     #         # print('params_dict', params_dict)
#     #         exec_job(conn, schm_tb_nm, job_type, job_params=params_dict,
#     #                  check_tb_strct=params_dict.get('check_tb_strct', True))
#     #         return schm_tb_nm
#     #     else:
#     #         raise Exception("输入不能为空")
#
#     depend_job_set = set()  # 依赖的job set 集
#     dsk = {'start': (start_flow,)}
#     for i in df.iterrows():
#         tp1 = (exec_job_by_depend, i[1]['command'])
#         depend_job_set.update(i[1]['dependencies'])
#         tp2 = tuple(i[1]['dependencies'])
#         dsk[i[0]] = tp1 + tp2
#
#     tp = (end_flow,)
#     dsk['end'] = tp + tuple(set(dsk.keys()) - depend_job_set)
#
#     # print(dsk)
#     # keys = ['fpdp_emp_stats_sum']
#     # dsk2, dependencies = cull(dsk, keys)
#     # print(dependencies)
#     # print("dsk2", dsk2, dependencies)
#     # dsk3, dependencies = fuse(dsk2, keys, dependencies)
#     # print(dsk3)
#     # keyorder = order(dsk)
#     # state = start_state_from_dask(dsk3, cache=None, sortkey=keyorder.get)
#     # print(state)
#     get(dsk, 'end')


def que_test():
    q = Queue(maxsize=0)

    # 写入队列数据
    q.put(0)
    q.put(1)
    q.put(2)

    # 输出当前队列所有数据
    # print(q.queue)
    # 删除队列数据，并返回该数据
    # q.get()
    # 输也所有队列数据
    # print(q.queue)
    while not q.empty():
        t = q.get()
        if t == 1:
            q.put(3)
        print(t)
        # print(q.queue, len(q.queue))


def exec_cmd(cmd, job_nm):
    rs = exec_shell(cmd, log_stdout=True)
    if rs['returncode'] == 0:
        return job_nm
    else:
        raise Exception("start_flow 执行错误:%s" % str(rs['stderr']))


class JobFlow(object):

    def __init__(self, batch_nm, batch_id=None):
        self.batch_nm = batch_nm
        if batch_id:
            self.batch_id = str(batch_id)
        else:
            self.batch_id = datetime.now().strftime('%Y%m%d%H%M%S') + "_" + str(self.batch_nm)
        self.job_map_to_tb = None

    def start_flow(self):
        logger.info("开始flow执行,batch_nm:%s,batch_id:%s 生成参数" % (self.batch_nm, self.batch_id,))
        cmd = "etl.py etl_params {batch_nm} -batch_id {batch_id} -c False"
        rs = exec_cmd(cmd.format(batch_nm=self.batch_nm, batch_id=self.batch_id), 'start')
        return rs

    def end_flow(self, *args):
        logger.info("batche_id:%s,flow执行完成,%s" % (self.batch_id, str(args)))
        return 'end'

    def get_jobs(self):
        """返回作业列表，
        返回的是个dataframe类型 columns包含job_nm,dependencies,command"""
        # def get_jobs(path='/home/xzh/dps/etl/dpsetl/dpsetl/dw_daily.csv'):
        config_file = os.path.join(flows_path, self.batch_nm)
        config_file = config_file + ".csv"
        df = pd.read_csv(config_file, index_col='job_nm')
        df.fillna({'dependencies': 'start'}, inplace=True)
        df['dependencies'] = df['dependencies'].apply(lambda x: re.split('\W', str(x).strip()))
        df['tb_nm'] = df['command'].apply(lambda x: x.split()[1])
        self.job_map_to_tb = df.to_dict()['tb_nm']
        # print(df['dependencies'])
        return df

    def get_flow(self):
        df = self.get_jobs()
        depend_job_set = set()  # 依赖的job set 集
        dsk = {'start': (self.start_flow,)}
        for i in df.iterrows():
            tp1 = (self._exec_job_by_depend, (i[1]['command'], i[0]))  # (i[1]['command'], i[0])
            depend_job_set.update(i[1]['dependencies'])
            tp2 = tuple(i[1]['dependencies'])
            dsk[i[0]] = tp1 + tp2
        end = (self.end_flow,)
        dsk['end'] = end + tuple(set(dsk.keys()) - depend_job_set)
        return dsk

    def check_dependencies_stat(self, conn, dependencies, schm_tb_nm):
        if self.batch_id and dependencies:
            # print(dependencies)
            # dependencies = list(dependencies)
            logger.debug("检查表%s依赖情况" % schm_tb_nm)
            check_count = 1
            check_flag = False
            if 'start' in dependencies:
                dependencies.remove('start')
            if self.job_map_to_tb is None:  # 为空时重新读取数据
                self.get_jobs()
            if dependencies:
                # print(dependencies, self.job_map_to_tb)
                dependencies_list = list(map(lambda x: self.job_map_to_tb.get(x), dependencies))
                # print("", schm_tb_nm, dependencies, dependencies_list)
                tb_nms = "','".join(dependencies_list)
                sql = """select tb_nm,batch_stat stat from {log_tb_nm} 
                             where batch_id='{batch_id}' and tb_nm in('{tb_nm}') """
                sql = sql.format(batch_id=self.batch_id, log_tb_nm=tb_task_log, tb_nm=tb_nms)
                while not check_flag:
                    df = conn.select(sql)
                    if df.shape[0] > 0:
                        df.set_index("tb_nm", inplace=True)
                        tb_stat = df.to_dict()['stat']
                        tp = set(list(map(lambda x: tb_stat.get(x.strip(), -1), dependencies_list)))
                        if max(tp) > 1:  # 存在作业错误
                            raise Exception("依赖作业出现错误:%s," % str(tb_stat))
                        if len(tp) == 1 and list(tp)[0] == 1:
                            check_flag = True
                            logger.info("%s依赖作业检查通过,开始执行" % schm_tb_nm)
                        else:
                            logger.warning("依赖作业检查不通过:%s,等待稍后重试" % str(tb_stat))
                            time.sleep(20)
                    check_count += 1
            else:
                logger.info("%s 无依赖，直接执行" % schm_tb_nm)
        else:
            logger.warning("batch_id为空，无状态可查")

    def _exec_job_by_depend(self, *args):
        if args:
            # print("args", args)
            cmd = args[0][0]
            if 'batch_id' in cmd:
                # cmd = cmd.replace('${', '{')
                # cmd = cmd.format(batch_id=self.batch_id)
                raise Exception("flow处理模式下batch_id 不能在单独作业命令中指定")
            else:
                cmd = cmd + ' -batch_id ' + str(self.batch_id) + ' -batch_nm ' + str(self.batch_nm)
            job_nm = args[0][1]
            conn, schm_tb_nm, job_type, params_dict = parse_sys_args(cmd)
            # params_dict['batch_id'] = self.batch_id
            dependencies = list(args[1:]).copy()
            self.check_dependencies_stat(conn, dependencies, schm_tb_nm)
            exec_cmd(cmd, job_nm)
            logger.info("%s 执行完成 \n" % schm_tb_nm)
            # exec_job(conn, schm_tb_nm, job_type, job_params=params_dict,
            #          check_tb_strct=params_dict.get('check_tb_strct', True))
            return job_nm
        else:
            raise Exception("输入不能为空")

    def exec_flow(self):
        """执行flow"""
        # print(dsk['T1'])
        # keyorder = order(dsk)
        # # print(keyorder)
        # state = start_state_from_dask(dsk, cache=None, sortkey=keyorder.get)
        # print(state)
        try:
            crt_el_log_tb(Conn())  # 检查日志表结构变化
            dsk = self.get_flow()
            get(dsk, 'end', num_workers=8)  # 执行任务
        except Exception as e:
            from etls.comm.emailcomm import send_error_msg
            logger.error(str(e))
            # send_error_msg(str(e), "flow执行错误%s" % self.batch_nm, if_to_wx=False)
            raise e


if __name__ == '__main__':
    # print(config.get("pool", None))
    # context = get_context()
    # pool = context.Pool(4, initializer=initialize_worker_process)
    # print(pool.apply_async())
    # import dask
    #
    # dask.config
    # jf = JobFlow('/home/xzh/dps/etl/dpsetl/dpsetl/dw_daily.csv', 2020082417020669, batch_nm='T1')
    # print(jf.batch_id, jf.job_map_to_tb)
    # jf.get_jobs()
    # jf.exec_flow()
    # print(jf.batch_id, jf.job_map_to_tb)
    jf = JobFlow('T1')
    jf.exec_flow()
    pass
