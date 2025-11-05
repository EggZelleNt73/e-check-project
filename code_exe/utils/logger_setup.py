import os, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

def get_logger(name: str="SparkJobLogger") -> logging.Logger:
    """Return a configured logger shared across modules."""
    LOG_DIR = "/opt/spark/logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(LOG_DIR, f"spark_job_{datetime.now().strftime('%Y_%m_%d_%H-%M-%S')}.log")
    
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            handlers=[
                RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=10), # file logs
                logging.StreamHandler() # console logs
            ]
        )

    return logging.getLogger(name)

if __name__ == "__main__":
    get_logger(None)