import logging

log_buffer = []

class ListHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

def get_logger(portal):
    logger = logging.getLogger(portal)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        list_handler = ListHandler()
        list_handler.setFormatter(formatter)

        logger.addHandler(list_handler)

    return logger