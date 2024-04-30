from loguru import logger


class S3Downloader:
    def __init__(self):
        pass

    def download_files(self, s3_client, bucket: str, local_path: str, s3_files: list[str]):
        for file in s3_files:
            filename = file.split("/")[-1]
            with open(f'{local_path}/{filename}', "wb") as file_obj:
                # Writing s3 file content into local file
                s3_client.download_fileobj(bucket, file, file_obj)
        logger.info("**********Successfully downloaded all files from S3***********")
