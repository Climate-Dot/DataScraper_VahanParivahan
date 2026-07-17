import logging
import warnings


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

NOISY_LOGGERS = (
    "WDM",
    "webdriver_manager",
    "urllib3",
    "selenium",
    "azure",
)

OPENPYXL_NO_STYLE_WARNING = "Workbook contains no default style, apply openpyxl's default"


def configure_pipeline_logging(level=logging.INFO):
    """Keep pipeline logs focused on project events instead of library chatter."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    formatter = logging.Formatter(LOG_FORMAT)
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
    else:
        for handler in root_logger.handlers:
            handler.setFormatter(formatter)

    for logger_name in NOISY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    warnings.filterwarnings(
        "ignore",
        message=OPENPYXL_NO_STYLE_WARNING,
        category=UserWarning,
    )
