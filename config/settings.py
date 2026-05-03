import logging
import logging.handlers
import os
import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH  = PROJECT_ROOT / 'config' / 'config.yaml'

def get_config():
    with open(CONFIG_PATH, 'r') as f:
        cfg = yaml.safe_load(f)
    cfg['database']['password'] = os.getenv('DB_PASSWORD', 'changeme')
    return cfg

def get_logger(name):
    cfg = get_config()
    log_cfg = cfg['logging']
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(log_cfg['level'])
    formatter = logging.Formatter(log_cfg['format'])
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    log_path = PROJECT_ROOT / log_cfg['file']
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(log_path, maxBytes=log_cfg['max_bytes'], backupCount=log_cfg['backup_count'])
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
