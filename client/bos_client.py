import logging

from baidubce.auth.bce_credentials import BceCredentials
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.exception import BceError
from baidubce.services.bos.bos_client import BosClient

from tools import common


class BOSClient(object):
    def __init__(self, bos_conf):
        __config = BceClientConfiguration(credentials=BceCredentials(bos_conf['ak'], bos_conf['sk']),
                                          endpoint=bos_conf['host'])
        __config.connection_timeout_in_mills = bos_conf.get('conn_timeout_ms', 5000)
        self._bos_client = BosClient(__config)
        self.logger = logging.getLogger()

    def put_file(self, bucket_name, object_key, file_path):
        try:
            common.retryer(self._bos_client.put_object_from_file, bucket_name, object_key, file_path)
        except BceError as err:
            self.logger.error(err)
        else:
            return True
        return False

    def get_file(self, bucket_name, object_key, file_path):
        common.retryer(self._bos_client.get_object_to_file, bucket_name, object_key, file_path)

    def get_object_as_string(self, bucket_name, object_key):
        return common.retryer(self._bos_client.get_object_as_string, bucket_name, str(object_key))
