# cython: language_level=3
from etls.comm.loggers import get_logger
from etls.conf.settings import setting_file_path
import os

logger = get_logger()
import yaml

# 获取 etls/conf/params.yaml 参数文件中的参数

PARAMS_FILE_PATH = setting_file_path.replace("settings.py", "DBmapping")


def get_db_mapping(from_db_type, to_db_type, mapping_type='data_type_mapping'):
    file_path = PARAMS_FILE_PATH + "/{0}2{1}.yaml".format(from_db_type.lower(), to_db_type.lower())
    if os.path.exists(file_path):
        mapping = yaml.load(open(file_path), Loader=yaml.FullLoader)
        return mapping[mapping_type]
    else:
        raise Exception("不存在的mapping文件：" + file_path)



