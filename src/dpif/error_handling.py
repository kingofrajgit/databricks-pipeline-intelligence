import logging
import sys
from pathlib import Path
from typing import Any

from dpif.models import Severity

from .config import get_settings


def setup_logging(
    name: str = "dpif",
    level: str | None = None,
    log_file: str | None = None,
) -> logging.Logger:
    """Set up structured logging for the framework."""
    settings = get_settings()

    # Determine log level
    if level is None:
        level = settings.debug and "DEBUG" or "INFO"

    # Determine log file
    if log_file is None:
        # Log to stdout primarily, optionally to file
        log_file = f"dpif-{name.lower()}.log"

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level, logging.INFO))

    # Avoid adding handlers if already configured
    if logger.handlers:
        return logger

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level, logging.INFO))

    # Format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(getattr(logging, level, logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Module-level logger
logger = setup_logging()


class DPIFError(Exception):
    """Base exception for all DPIF-related errors."""

    error_code: str = "DPIF_ERROR"
    severity: Severity = Severity.MEDIUM

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        severity: Severity | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.error_code
        self.severity = severity or self.severity
        self.kwargs = kwargs
        logger.error(
            f"DPIF Error [{self.error_code}]: {message}",
            extra={"error_code": self.error_code, "severity": self.severity.value},
        )


class ConfigurationError(DPIFError):
    """Raised when configuration is invalid or missing."""

    error_code: str = "CONFIG_ERROR"

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, error_code=self.error_code, severity=Severity.HIGH, **kwargs)


class ValidationError(DPIFError):
    """Raised when pipeline validation fails."""

    error_code: str = "VALIDATION_ERROR"

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, error_code=self.error_code, severity=Severity.MEDIUM, **kwargs)


class RuleEvaluationError(DPIFError):
    """Raised when a rule cannot be evaluated."""

    error_code: str = "RULE_EVALUATION_ERROR"

    def __init__(self, message: str, rule_id: str = "", **kwargs: Any) -> None:
        super().__init__(
            message,
            error_code=self.error_code,
            severity=Severity.MEDIUM,
            rule_id=rule_id,
            **kwargs,
        )


class InsufficientDataError(DPIFError):
    """Raised when insufficient data is available for analysis."""

    error_code: str = "INSUFFICIENT_DATA"

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(
            message,
            error_code=self.error_code,
            severity=Severity.INFO,
            **kwargs,
        )


class RuntimePredictionError(DPIFError):
    """Raised when runtime prediction cannot be made."""

    error_code: str = "RUNTIME_PREDICTION_UNAVAILABLE"

    def __init__(
        self, reason: str = "Insufficient historical execution data", **kwargs: Any
    ) -> None:
        super().__init__(
            reason,
            error_code=self.error_code,
            severity=Severity.INFO,
            **kwargs,
        )
