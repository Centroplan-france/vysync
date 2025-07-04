
def init_logger(name, default_level=logging.INFO):
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        level=default_level,
    )
    return logging.getLogger(name)
