from loguru import logger


class MoveFiles:
    def __init__(self, client):
        self.client = client

    def move_processed_files(self, bucket_name: str, source_prefix: str, destination_prefix: str):
        try:
            files_to_delete = []

            files_to_move = self.client.list_objects(
                Bucket=bucket_name,
                Prefix=source_prefix
            )
            if "Contents" in files_to_move:
                for file in files_to_move["Contents"]:
                    try:
                        source = f"{source_prefix}{file['Key']}"
                        destination = f"{destination_prefix}{file['Key']}"
                        self.client.copy_object(
                            Bucket=bucket_name,
                            CopySource={'Bucket': bucket_name, 'Key': source},
                            Key=destination
                        )
                        logger.info(f"Successfully copied file {file['Key']} to processed folder")
                        files_to_delete.append({
                            "Key": source
                        })
                    except Exception as error:
                        logger.error(f"Error when copying file {file['Key']} for processed. Error is: {error}")

            if files_to_delete:
                try:
                    self.client.delete_objects(
                        Bucket=bucket_name,
                        Delete={
                            "Objects": files_to_delete
                        }
                    )
                except Exception as error:
                    logger.error(f"Error when deleting objects. Error is: {error}")

        except Exception as error:
            logger.error(f"Error when moving files to processed folder. Error is: {error}")
