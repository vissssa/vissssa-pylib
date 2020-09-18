import logging

from minio import Minio, ResponseError


class MinioClient:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.__minio_client = Minio(endpoint=endpoint,
                                    access_key=access_key,
                                    secret_key=secret_key,
                                    secure=secure)
        self.logger = logging.getLogger()

    def create_bucket(self, bucket_name, location="us-east-1"):
        try:
            self.__minio_client.make_bucket(bucket_name, location=location)
        except ResponseError as err:
            self.logger.error(err)
        else:
            return True
        return False

    def list_buckets(self):
        buckets = self.__minio_client.list_buckets()
        return [bucket.name for bucket in buckets]

    def bucket_exists(self, bucket_name):
        return self.__minio_client.bucket_exists(bucket_name)

    def put_file(self, bucket_name, object_key, file_path):
        try:
            self.__minio_client.fput_object(bucket_name, object_key, file_path)
        except ResponseError as err:
            self.logger.error(err)
        else:
            return True
        return False

    def get_file(self, bucket_name, object_key, file_path):
        try:
            self.__minio_client.fget_object(bucket_name, object_key, file_path)
        except ResponseError as err:
            self.logger.error(err)
        else:
            return True
        return False
