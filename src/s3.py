# -*- coding: utf-8 -*-
import logging
import os

import boto3

LOGGER = logging.getLogger(__name__)


class S3:
    def __init__(self):
        self._s3 = boto3.client("s3", os.getenv("AWS_REGION"))

    def upload_to_s3(self, key: str, data):
        """
        upload an object to s3 bucket
        :param key: the key to identify the object to be uploaded
        :param data: the object to upload
        """
        self._s3.put_object(Body=data, Key=key, Bucket=os.getenv("BUCKET_NAME"))
        LOGGER.info(f"Uploaded {key} to S3 bucket")

    def download_from_s3(self, key: str):
        """
        download an object from s3 bucket
        :param key: the key to identify the object to be downloaded
        :return: the object data
        """
        response = self._s3.get_object(Bucket=os.getenv("BUCKET_NAME"), Key=key)
        return response.get("Body")

    def delete_from_s3(self, key: str):
        """
        delete an object in s3 bucket
        :param key: the key to identify the object to be deleted
        """
        self._s3.delete_object(Key=key, Bucket=os.getenv("BUCKET_NAME"))

    def list_bucket_objects(self, prefix=None):
        """
        list all the object keys in s3 bucket with specific prefix
        :param prefix: prefix used to search objects
        :return list of keys with given prefix
        """
        return [
            content["Key"]
            for content in self._s3.list_objects(Bucket=os.getenv("BUCKET_NAME"), Prefix=prefix)["Contents"]
        ]
