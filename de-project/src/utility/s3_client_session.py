import boto3
from loguru import logger


class S3Client:
    def __init__(self, access_key: str = None, secret_key: str = None, region: str = None):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def get_client(self):
        if not (self.access_key and self.secret_key and self.region):
            raise ValueError("S3 credentials access_key/secret_key/region not found")

        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )
        s3_client = session.client(
            service_name="s3"
        )
        logger.info("S3 client created successfully")
        return s3_client
