from glob import glob
import os
from utils.logger_setup import get_logger

logger = get_logger(__name__)

def delete_source_files_func(ext, directory):
    try:
        for file in glob(os.path.join(directory, ext)):
            os.remove(file)
            logger.info(f"Deleted file: {file}")
    except Exception as e:
        logger.error(f"Failed to clean source directory: {e}", exc_info=True)
    else:
        logger.info("Source directory cleaned successfully")

if __name__ == "__main__":
    delete_source_files_func()
