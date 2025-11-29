import logging
import sys


def configure_dependency_logger(name: str, level: int = logging.WARNING) -> None:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(level)


def configure_logging(
    console: bool = True,
    console_level: int = logging.WARNING,
    file: bool = True,
    file_level: int = logging.INFO,
    log_file: str = "ISPyPSA.log",
) -> None:
    """Configures ISPyPSA logging

    Examples:
        Perform required imports.
        >>> import logging
        >>> from ispypsa.logging import configure_logging

        Configure logging with default settings (console warnings, file info).
        >>> configure_logging()

        Configure logging with custom settings.
        >>> configure_logging(
        ...     console=True,
        ...     console_level=logging.INFO,
        ...     file=True,
        ...     file_level=logging.DEBUG,
        ...     log_file="my_run.log"
        ... )

        Disable file logging.
        >>> configure_logging(file=False)

    Args:
        console: Whether to log to the console. Defaults to True.
        console_level: Level of the console logging. Defaults to logging.WARNING.
        file: Whether to log to a log file. Defaults to True.
        file_level: Level of the file logging. Defaults to logging.INFO.
        log_file: Name of the logging file. Defaults to "ISPyPSA.log".

    Returns:
        None
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
    configure_dependency_logger("pypsa", logging.INFO)
