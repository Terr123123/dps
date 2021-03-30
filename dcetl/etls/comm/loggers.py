# encoding: utf-8
"""
@author: xuzh
"""
import time
import os
import logging.config
import yaml
from etls.conf.settings import log_level, log_home,log_config_file

dates = time.strftime('%Y%m%d', time.localtime(time.time()))
log_path = os.path.join(log_home, dates)  # 如果不存在这个logs文件夹，就自动创建一个
if not os.path.exists(log_path):
    os.makedirs(log_path)
default_log_file = os.path.join(log_path, 'etl.log')

with open(log_config_file,encoding='utf-8') as log:
    yaml_str = log.read()
    yaml_str = yaml_str.format(**locals())
LOGGING = yaml.load(yaml_str, Loader=yaml.FullLoader)

# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,
#     'formatters': {
#         # 日志格式
#         'standard': {
#             'format': '[%(asctime)s] %(levelname)-5.5s [%(filename)s:%(lineno)s] %(message)s'},
#         'simple': {  # 简单格式
#             'format': '[%(asctime)s] %(levelname)-5.5s %(message)s'
#         },
#     },
#     # 过滤
#     'filters': {
#     },
#     'handlers': {
#         # 默认记录所有日志
#         'default': {
#             'level': 'INFO',
#             'class': 'logging.handlers.RotatingFileHandler',
#             'filename': default_log_file,
#             'maxBytes': 1024 * 1024 * 5,  # 文件大小
#             'backupCount': 5,  # 备份数
#             'formatter': 'standard',  # 输出格式
#             'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
#         },
#         # # 输出错误日志
#         # 'error': {
#         #     'level': 'ERROR',
#         #     'class': 'logging.handlers.RotatingFileHandler',
#         #     'filename': os.path.join(log_path, 'error.log'),
#         #     'maxBytes': 1024 * 1024 * 5,  # 文件大小
#         #     'backupCount': 5,  # 备份数
#         #     'formatter': 'standard',  # 输出格式
#         #     'encoding': 'utf-8',  # 设置默认编码
#         # },
#         # 控制台输出
#         'console': {
#             'level': 'DEBUG',
#             'class': 'logging.StreamHandler',
#             'formatter': 'standard'
#         },
#         # 输出info日志
#         'timerotalog': {
#             'class': 'logging.handlers.TimedRotatingFileHandler',
#             'filename': default_log_file,
#             'when': 'midnight',
#             'encoding': 'utf-8',
#             'formatter': 'standard',
#         },
#
#     },
#     'loggers': {
#         '': {  # 默认
#             'handlers': ['console'],
#             'level': log_level,
#             'propagate': False
#         },
#         'root': {
#             'handlers': ['console', 'default'],
#             'level': log_level,
#             'propagate': False
#         },
#     }
# }

logging.config.dictConfig(LOGGING)


def get_handler(job_nm='etl'):
    """
    根据作业新增日志文件，文件名和日志作业一致
    :param job_nm:
    :return:
    """
    dates_tp = time.strftime('%Y%m%d', time.localtime(time.time()))
    log_path_tp = os.path.join(log_home, dates_tp)
    if not os.path.exists(log_path_tp):
        os.makedirs(log_path_tp)
    # job_nm = job_nm + '_' + time.strftime('%H%M%S', time.localtime(time.time()))
    log_file = os.path.join(log_path_tp, job_nm + '.log')
    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)-5.5s [%(filename)s:%(lineno)s] %(message)s')
    # pid:%(process)-5s  获取进程ID
    fh.setFormatter(formatter)
    return fh


def get_logger(nm='etl'):
    """
    初始化日志
    :param nm: 指定logger
    :return:
    """
    # logging._srcfile = None # 把文件名（filename）清空
    logging.logThreads = 0  # 优化节省日志计算时间
    logging.logMultiprocessing = 0
    logging.logProcesses = 0
    logging.config.dictConfig(LOGGING)
    logger = logging.getLogger(nm)
    if len(nm) > 4:
        fh = get_handler(nm)  # 输出到特定的日志文件
        logger.addHandler(fh)
    return logger
