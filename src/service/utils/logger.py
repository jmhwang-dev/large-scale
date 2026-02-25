import logging
from logging import Logger

def get_logger(_name: str, _filename: str, _filemode: str = 'w'):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=_filename,
        filemode=_filemode)
    return logging.getLogger(_name)

def write_log(logger: Logger, message: str):
    logger.error(message)