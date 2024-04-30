from loguru import logger


class CopyInvalidFiles:
    def __init__(self):
        pass

    def copy_invalid_files(self, s3_client, bucket_name: str, source_prefix: str, destination_prefix: str,
                           invalid_files: list):
        for invalid_file in invalid_files:
            source = f'{source_prefix}{invalid_file}'
            destination = f'{destination_prefix}{invalid_file}'
            try:
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': source},
                    Key=destination
                )
            except Exception as err:
                logger.error(f"Error when copying file {source} to {destination}. Error is: {err}")
