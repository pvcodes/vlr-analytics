import logging
from pathlib import Path


def get_logger(name: str = "vlr-etl") -> logging.Logger:
    """
    Get a configured logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logs_path = Path.cwd().parent.parent / "logs/scraping.log"
    logs_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(logs_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger


class ContextLogger:
    """
    A logger that prepends proxy and worker context.
    """

    def __init__(self, logger: logging.Logger, proxy: str = "NA", worker: str = "NA"):
        self._logger = logger
        self.proxy = proxy
        self.worker = worker

    def _log(self, level: int, message: str, *args, **kwargs):
        ctx = f"{self.proxy} | {self.worker}"
        self._logger.log(level, f"{ctx} | {message}", *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        self._log(logging.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        self._log(logging.INFO, message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        self._log(logging.WARNING, message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        self._log(logging.ERROR, message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        self._log(logging.CRITICAL, message, *args, **kwargs)

    def exception(self, message: str, *args, **kwargs):
        self._log(logging.ERROR, message, *args, **kwargs)

    def set_proxy(self, proxy: str):
        self.proxy = proxy

    def set_worker(self, worker: str):
        self.worker = worker


def get_logger_with_context(proxy: str = "NA", worker: str = "NA") -> ContextLogger:
    """
    Get a logger with proxy and worker context.
    """
    base_logger = get_logger()
    return ContextLogger(base_logger, proxy, worker)


logger = get_logger()
