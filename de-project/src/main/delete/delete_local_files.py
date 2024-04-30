from loguru import logger
import os
import shutil


def delete_files_from_local(directory: str):
    try:
        for dir_or_file in os.listdir(directory):
            item = f"{directory}/{dir_or_file}"
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                shutil.rmtree(item)
    except Exception as error:
        logger.error(f"Error when deleting files from local. Error is: {error}")
