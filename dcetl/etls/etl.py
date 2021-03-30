#!/usr/bin/env python3
from etls.comm.loggers import get_handler,get_logger
import sys
from etls.comm.job import exec_job, parse_sys_args
from etls.comm.jobflow import JobFlow
# import logging
# logger = logging.getLogger()
logger = get_logger()

if __name__ == '__main__':
    if len(sys.argv) >= 3:
        # 命令样式 etl.py tb_nm job_type 后面跟着 一一对应的kv参数(个数不限)
        # tb_nm  表名 例如 dw.dim_cust(可以是多个表名用“,”分割，不能有空格)
        # job_type job类型 可以取ddl/dml/batch_el/func/el/sql/flow, func是指对应python文件中的函数名称 el指同步表,batch_el批次同步
        # 参数格式 参数名称需要以-或者--开头然后紧跟着参数值,例如-start_dt 2019-10-01 或者 --start_dt 2019-10-01
        # etl.py  task_infos deal -sys_dt 2019-12-10 执行目录下task_infos.py python文件中的deal函数 sys_dt是deal函数的参数
        # etl.py  edw.dim_cust dml -batch_dt 2019-12-10 执行的edw/目录下的 dw.dim_cust.sql  batch_dt 是传入参数
        # etl.py  edw.fact_cust_leads dml -schm rtp -p_days 10
        # etl.py hq_hr.estatus el -conn ibm -src_tb_nm ecology.estatus -src_db_cd hr
        # etl.py T1 batch_el -batch_id 202008200101 和 etl.py batch_el el -batch_id 202008200101 -batch_nm T1 等效
        # etl.py T1 flow 执行T1批处理
        # 临时导入一张表
        # python3 etl.py hq_hr.estatus el -conn ibm -src_tb_nm ecology.estatus -src_db_cd hr -c False

        # 解析传入参数
        conn, schm_tb_nm, job_type, params_dict = parse_sys_args(sys.argv)
        log_file_nm = schm_tb_nm
        log_file_nm = log_file_nm.split('/')[-1]
        fh = get_handler(log_file_nm)  # 输出到特定的日志文件
        try:
            if job_type == 'flow':
                # jobflow工作流批处理整条链路的作业
                logger.info("按flow工作流批处理整条链路的作业:%s" % schm_tb_nm)
                jf = JobFlow(batch_nm=schm_tb_nm, batch_id=params_dict.get('batch_id', None))
                jf.exec_flow()
            else:
                logger.addHandler(fh)
                logger.info("执行命令：" + " ".join(sys.argv))
                # 常规job
                tbs = schm_tb_nm.split(',')  # 如果是多个表
                for schm_tb_nm in tbs:
                    exec_job(conn, schm_tb_nm, job_type, job_params=params_dict,
                             check_tb_strct=params_dict.get('check_tb_strct', True))
        finally:
            logger.info("end...etl.py %s %s" % (schm_tb_nm, job_type))
            logger.info("\n\n")
            logger.removeHandler(fh)
            conn.close()
    else:
        raise Exception("参数输入不正确，样例：etl.py edw.dim_cust dml -batch_dt 2019-12-10 或 etl.py T1 flow")
