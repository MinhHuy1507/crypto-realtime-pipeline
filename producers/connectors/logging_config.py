import logging
import sys

def setup_logging(name: str = "crypto_producer"):
    """
    Configures and returns a logger with a standard format.
    """
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(name)
