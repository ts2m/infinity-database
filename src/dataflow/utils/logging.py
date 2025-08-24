import logging, sys, os
def get_logger(name: str):
    logger = logging.getLogger(name)
    if logger.handlers: return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fm = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
    h.setFormatter(fm)
    logger.addHandler(h)
    return logger
