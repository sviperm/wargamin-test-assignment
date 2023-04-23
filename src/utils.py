import logging

import psycopg2


def get_conn_cursor(db_config: dict[str, str]) -> tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor]:
    """
    Create a connection and cursor to the PostgreSQL database.

    Args:
        db_config: A dictionary containing the database configuration parameters.

    Returns:
        A tuple containing the connection and cursor objects.
    """
    conn = psycopg2.connect(
        dbname=db_config['dbname'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    cursor = conn.cursor()
    return conn, cursor


def setup_loggers() -> tuple[logging.Logger, logging.Logger]:
    """
    Set up the basic logger and the skipped elements logger.

    Returns:
        A tuple containing the basic_logger and skipped_logger instances.
    """
    # Set up the basic logger
    basic_logger = logging.getLogger()
    basic_logger.setLevel(logging.INFO)
    basic_handler = logging.FileHandler('logs/logs.log')  # Updated file path
    basic_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    basic_logger.addHandler(basic_handler)

    # Set up the skipped elements logger
    skipped_logger = logging.getLogger('skipped_logger')
    skipped_logger.setLevel(logging.WARNING)
    skipped_handler = logging.FileHandler('logs/skipped_elements.log')  # Updated file path
    skipped_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    skipped_logger.addHandler(skipped_handler)

    return basic_logger, skipped_logger


def safe_float_convert(n: str | float) -> float:
    if isinstance(n, float):
        return n
    return float(n.replace(',', '.'))
