import time
import os
from etls.comm.properties import Properties
from etls.comm.datefunc import cast_strs_to_time
from etls.conf.settings import TEMP_HOME
# from etl.comm.loggers import get_logger
import json
import logging
logger = logging.getLogger()
# logger = get_logger()


def get_batch_param_from_file(batch_id, in_param=None):
    config_file = os.path.join(TEMP_HOME, str(batch_id) + '.json')
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            params = json.load(f)
    else:
        logger.warning("%s中没有找到参数配置文件%s" % (TEMP_HOME, config_file))
        params = {'batch_id': batch_id}
    if in_param:
        params.update(in_param)
    if 'batch_dt' not in params:
        tp = cast_strs_to_time(batch_id)  # 尽可能的转化为日期
        if tp and tp.tm_year > 1900:  # 能获取有效日期
            params['batch_dt'] = time.strftime("%Y-%m-%d", tp)
        else:
            params['batch_dt'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 不成功取当前日期
    return params


def init_params() -> dict:
    """
    读取azkaban传进来的参数文件
    :return:
    """
    azkaban_prop_file = os.getenv("JOB_PROP_FILE")
    if azkaban_prop_file:  # 如果获取到azkaban上个节点任务传入的参数文件
        prop = Properties(azkaban_prop_file)
        prop.get_properties()
        params = prop.to_dict()
    else:
        params = {}
    logger.debug(params)
    return params


def get_batch_dt(batch_id, params, input_params):
    """
    获取batch_dt
    :param batch_id:
    :param params:
    :param input_params:
    :return:
    """
    if params and 'batch_dt' in params:
        return params['batch_dt']
    elif input_params and 'batch_dt' in input_params:
        return input_params['batch_dt']
    else:
        tp = cast_strs_to_time(batch_id)  # 尽可能的转化为日期
        if tp and tp.tm_year > 1900:  # 获得有效日期
            return time.strftime("%Y-%m-%d", tp)
        else:
            logger.warning("batch_id:%s 的batch_dt取当前日期 " % batch_id)
            return time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 不成功取当前日期


class BatchParams(object):
    def __init__(self, batch_id, input_params=None):
        """
        初始化对象
        :param batch_id: yyyymmddhhmmss
        :param input_params: 不覆盖self.params,只对读出的数据覆盖 dict {} 一般都需要传入一个 batch_dt: yyyy-mm-dd
        """
        self.batch_id = batch_id
        self.azkaban_params = init_params()
        self.azkaban_params['batch_id'] = batch_id
        self.params = self.azkaban_params
        self.config_file = os.path.join(TEMP_HOME, str(batch_id) + '.json')
        # self.batch_dt = None  # get_batch_dt(batch_id, self.azkaban_params, input_params)
        # input_params['batch_dt'] = self.batch_dt
        self.input_params = input_params
        logger.debug("batch_id:%s 参数文件路径:%s" % (self.batch_id, self.config_file))

    def put(self, key, value):
        """
        增加参数对
        :param key:
        :param value:
        :return:
        """
        self.params[key] = value

    def read(self):
        """
        读取参数文件
        :return:
        """
        config_file = self.config_file
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                tp_params = json.load(f)
        else:
            logger.warning("batch_id:%s 没有找到参数配置文件%s" % (self.batch_id, config_file))
            tp_params = self.params
        if self.input_params:
            tp_params.update(self.input_params)
        if 'batch_dt' not in tp_params:
            tp = cast_strs_to_time(self.batch_id)  # 尽可能的转化为日期
            if tp and tp.tm_year > 1900:  # 能获取有效日期
                batch_dt = time.strftime("%Y-%m-%d", tp)
            else:
                logger.warning("batch_id:%s 的batch_dt取当前日期 " % self.batch_id)
                batch_dt = time.strftime('%Y-%m-%d', time.localtime(time.time()))  # 不成功取当前日期
            tp_params['batch_dt'] = batch_dt
        if 'etl_dtm' not in tp_params or 'etl_dt' not in tp_params:
            etl_dtm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            tp_params['etl_dt'] = etl_dtm[0:10]
            tp_params['etl_dtm'] = etl_dtm
        self.params = tp_params
        return tp_params

    def save(self, force=False):
        """
        保存参数文件
        :param force boolean 是否强制更新文件
        :return:
        """
        if os.path.exists(self.config_file) and force is False:
            logger.warning("参数文件已经存在不保存新内容，需要更新参数文件的请先把原文件备份成新文件名")
            return
        try:
            batch_dt = get_batch_dt(self.batch_id, self.params, self.input_params)
            self.put('batch_dt', batch_dt)
            etl_dtm = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            self.put('etl_dt', etl_dtm[0:10])
            self.put('etl_dtm', etl_dtm)
            if self.input_params:
                self.params.update(self.input_params)
            if 'batch_nm' not in self.params:
                raise Exception("batch_nm必须指定")
            with open(self.config_file, "w") as fout:
                json.dump(self.params, fout, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error((str(e)))
            os.remove(self.config_file)
            raise e
        logger.info("batch_id:%s保存参数文件%s" % (self.batch_id, self.config_file))


# if __name__ == '__main__':
#     batch_param = BatchParams("20200218282311")
#     # batch_param.put('batch_dt', '2020-08-18')
#     batch_param.put('ods', 'ods')
#     batch_param.put('batch_nm', "T1")
#     batch_param.put('p_days', 3600)
#     batch_param.put('参数名', '中文')
#     batch_param.save()  # 参数文件保存
