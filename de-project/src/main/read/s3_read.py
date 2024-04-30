from loguru import logger


class S3Reader:
    def __init__(self, s3_client: object, bucket_name: str, prefix: str) -> None:
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.prefix = prefix

    def read_files(self) -> list | None:
        """
        Fetch files from S3 bucket.
        :return: A list of S3 files that to be processed or None.
        """
        try:
            objects = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
        except Exception as err:
            logger.error(f"Error when fetching files from S3. Error is: {err}")
            return

        if 'Contents' in objects and objects["Contents"]:
            files = [content["Key"] for content in objects["Contents"] if not content['Key'].endswith("/")]
            if files:
                logger.info("Files fetched from s3")
                return files
            else:
                logger.warning("No file found in S3")
                return
        else:
            logger.warning("No content found from S3")
            return
