from loguru import logger
import traceback


class FileWriter:
    def __init__(self, mode: str, file_format: str):
        self.mode = mode
        self.file_format = file_format

    def write_dataframe(self, df, local_path: str):
        try:
            df.write.format(self.file_format). \
             option("header", "true"). \
             mode(self.mode). \
             option("path", local_path). \
             save()
        except Exception as error:
            logger.error(f"Error when writing dataframe into local system. Error is: {error}")
            traceback.format_exc()
