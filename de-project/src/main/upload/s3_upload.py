from datetime import datetime
import os
import traceback
from loguru import logger


class S3Uploader:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def upload_to_s3(self, local_dir: str, bucket_name: str, prefix: str):
        current_epoch = int(datetime.now().timestamp()) * 1000
        key_prefix = f"{prefix}/{current_epoch}"
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    self.s3_client.upload_file(
                        Filename=file_path,
                        Bucket=bucket_name,
                        Key=f"{key_prefix}/{file}"
                    )
                except Exception as error:
                    logger.error(f"Error when uploading file into s3 datamart. Error is: {error}")
                    traceback.format_exc()
