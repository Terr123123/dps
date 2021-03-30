"""
批次参数生成器，生成的参数会在对应的batch_id的批处理中使用。可不生成参数文件
注意：
 一个批次对应一个参数生成函数，如T1批次对应T1(batch_id),
而且这里的批次名和task_info里面的batch_nm要一致，不是凭空捏造的
"""

from datetime import datetime
from dpsetl.comm.batch import BatchParams
import logging
import inspect

logger = logging.getLogger()


def get_function_name():
    """获取正在运行函数(或方法)名称"""
    return inspect.stack()[1][3]


# 生成对应T1的相应批次的参数文件
def T1(batch_id):
    batch_nm = get_function_name()  # sys._getframe().f_code.co_name
    batch_param = BatchParams(batch_id)
    # batch_param.put('batch_dt', '2020-08-18')
    batch_param.put('ods', 'ods')
    batch_param.put('batch_nm', batch_nm)
    batch_param.put('p_days', 3600)
    batch_param.put('参数名', '中文')
    batch_param.save()  # 参数文件保存


def M5(batch_id):
    batch_nm = get_function_name()  # sys._getframe().f_code.co_name
    batch_param = BatchParams(batch_id)
    # batch_param.put('batch_dt', '2020-08-18')
    batch_param.put('ods', 'ods')
    batch_param.put('batch_nm', batch_nm)
    batch_param.put('p_days', 3600)
    batch_param.save()  # 参数文件保存


def params_test():
    batch_id = datetime.now().strftime('%Y%m%d%H%M%S%f')[0:16]
    M5(batch_id + 'M5')
    T1(batch_id + 'T1')


if __name__ == '__main__':
    params_test()
