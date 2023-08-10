import sys
import logging

DEFAULT_LOG_FMT = "'%(asctime)s %(levelname)s %(message)s'"


def get_logger(name: str, log_path=None, log_fmt=DEFAULT_LOG_FMT, console=True):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if log_path is not None:
        if not any(isinstance(hdlr, logging.FileHandler) and hdlr.baseFilename == str(log_path) for hdlr in logger.handlers):
            fh = logging.FileHandler(log_path)
            fh.setLevel(logging.INFO)
            fh.setFormatter(logging.Formatter(log_fmt))
            logger.addHandler(fh)

    if console:
        if not any(isinstance(hdlr, logging.StreamHandler) for hdlr in logger.handlers):
            ch = logging.StreamHandler(sys.stdout)  # set stream to stdout
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter(log_fmt))
            logger.addHandler(ch)

    return logger


def get_console_only_logger(name: str, log_fmt=DEFAULT_LOG_FMT):
    logger = get_logger(name, log_path=None, log_fmt=log_fmt, console=True)
    return logger


def file_only_logger(name: str, log_path, log_fmt=DEFAULT_LOG_FMT):
    logger = get_logger(name, log_path=log_path, log_fmt=log_fmt, console=False)
    return logger
