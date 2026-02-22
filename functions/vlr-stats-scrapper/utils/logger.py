import atexit
import logging
import sys

from utils.constants import ENVIRONMENT, APP_NAME


def setup_logger(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """Set up and return a logger with optional Google Cloud Logging integration."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)

    if ENVIRONMENT == "PRODUCTION":
        try:
            from google.cloud import logging as cloud_logging
            from google.cloud.logging.handlers import StructuredLogHandler

            client = cloud_logging.Client()
            cloud_handler = StructuredLogHandler()
            cloud_handler.setLevel(level)
            logger.addHandler(cloud_handler)

            # flush and close cleanly on shutdown
            atexit.register(cloud_handler.close)
            atexit.register(client.close)
        except Exception:
            pass

    return logger


logger = setup_logger(APP_NAME)
