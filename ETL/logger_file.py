import logging
from pathlib import Path
import os
from datetime import datetime


# Configure the logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def setup_logging_handler(file_dir):
    file_path = os.path.join(file_dir, f'{__name__}.log {datetime.now()}')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(file_path)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    return logger

def setup_logs():
    base_dir = Path(__file__).parents[0]
    log_directory = os.path.join(base_dir, 'logs')
    make_dir(log_directory)
    logger = setup_logging_handler(log_directory)
    return logger