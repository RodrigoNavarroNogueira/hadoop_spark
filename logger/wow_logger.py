import logging


def logger_conf(log_path: str) -> None:
    """
    Configures the logger according to the job that is executed
    
    param log_path: Path where the log file will be saved
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(filename)s: %(message)s',
        filename=log_path,
        encoding='utf-8',
        filemode='a',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(filename)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))

    logging.getLogger().addHandler(console_handler)
