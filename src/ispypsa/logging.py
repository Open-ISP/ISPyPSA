import logging
import sys


def configure_logging(
    console: bool = True,
    console_level: int = logging.WARNING,
    file: bool = True,
    file_level: int = logging.INFO,
    log_file: str = "ISPyPSA.log",
) -> None:
    """Configures ISPyPSA logging

    Args:
        console: Whether to log to the console. Defaults to True.
        console_level: Level of the console logging. Defaults to logging.WARNING.
        file: Whether to log to a log file. Defaults to True.
        file_level: Level of the file logging. Defaults to logging.INFO.
        log_file: Name of the logging file. Defaults to "ISPyPSA.log".
    """
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    handlers = []
    if console:
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)
        handlers.append(console_handler)
    if file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    if not handlers:
        handlers.append(logging.NullHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=handlers,
    )
