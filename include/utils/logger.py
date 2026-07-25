import logging

def setup_logger(name: str) -> logging.Logger:
    """Return the standard application logger for ``name``."""
    return logging.getLogger(name)
