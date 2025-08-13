"""
Retry logic for handling transient failures in inventory CLI operations.

This module provides decorators and utilities for implementing retry logic
with exponential backoff, jitter, and intelligent failure detection.
"""

import asyncio
import logging
import random
import time
from functools import wraps
from typing import Any, Callable, List, Optional, Type
from dataclasses import dataclass
from enum import Enum

from .exceptions import InventoryCLIError, ErrorCategory


logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """Backoff strategies for retry attempts."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_JITTER
    jitter_range: float = 0.1  # Jitter as fraction of delay (0.1 = ±10%)
    retryable_exceptions: List[Type[Exception]] = None
    retryable_error_categories: List[ErrorCategory] = None

    def __post_init__(self):
        """Set default retryable exceptions and categories."""
        if self.retryable_exceptions is None:
            self.retryable_exceptions = [
                ConnectionError,
                TimeoutError,
                OSError,
            ]

        if self.retryable_error_categories is None:
            self.retryable_error_categories = [
                ErrorCategory.CONNECTIVITY,
                ErrorCategory.DATABASE,
                ErrorCategory.SYSTEM,
            ]


class RetryContext:
    """Context for tracking retry attempts."""

    def __init__(self, operation_name: str, config: RetryConfig):
        self.operation_name = operation_name
        self.config = config
        self.attempt = 0
        self.total_delay = 0.0
        self.errors: List[Exception] = []
        self.start_time = time.time()

    def should_retry(self, error: Exception) -> bool:
        """Determine if an error should trigger a retry."""
        # Check if we've exceeded max attempts
        if self.attempt >= self.config.max_attempts:
            return False

        # Check if it's a retryable exception type
        if any(
            isinstance(error, exc_type) for exc_type in self.config.retryable_exceptions
        ):
            return True

        # Check if it's a retryable InventoryCLIError
        if isinstance(error, InventoryCLIError):
            if error.retryable:
                return True
            if error.category in self.config.retryable_error_categories:
                return True

        return False

    def calculate_delay(self) -> float:
        """Calculate delay for next retry attempt."""
        if self.config.backoff_strategy == BackoffStrategy.FIXED:
            delay = self.config.base_delay
        elif self.config.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self.config.base_delay * self.attempt
        elif self.config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (2 ** (self.attempt - 1))
        elif self.config.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER:
            base_delay = self.config.base_delay * (2 ** (self.attempt - 1))
            jitter = base_delay * self.config.jitter_range * (2 * random.random() - 1)
            delay = base_delay + jitter
        else:
            delay = self.config.base_delay

        # Cap at maximum delay
        delay = min(delay, self.config.max_delay)

        # Ensure minimum delay
        delay = max(delay, 0.1)

        return delay

    def record_attempt(self, error: Exception):
        """Record a failed attempt."""
        self.attempt += 1
        self.errors.append(error)

        logger.debug(
            f"Retry attempt {self.attempt}/{self.config.max_attempts} for {self.operation_name}: {error}"
        )

    def get_summary(self) -> dict:
        """Get summary of retry attempts."""
        return {
            "operation": self.operation_name,
            "total_attempts": self.attempt,
            "total_delay": self.total_delay,
            "duration": time.time() - self.start_time,
            "errors": [str(e) for e in self.errors],
        }


def retry_on_failure(
    config: Optional[RetryConfig] = None,
    operation_name: Optional[str] = None,
) -> Callable:
    """
    Decorator for adding retry logic to functions.

    Args:
        config: Retry configuration. Uses default if None.
        operation_name: Name of operation for logging. Uses function name if None.

    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            """Wrapper for synchronous functions."""
            op_name = operation_name or func.__name__
            context = RetryContext(op_name, config)

            while True:
                try:
                    result = func(*args, **kwargs)

                    if context.attempt > 0:
                        logger.info(
                            f"Operation {op_name} succeeded after {context.attempt} retries"
                        )

                    return result

                except Exception as e:
                    context.record_attempt(e)

                    if not context.should_retry(e):
                        logger.error(
                            f"Operation {op_name} failed after {context.attempt} attempts: {e}"
                        )
                        # Re-raise the last error
                        raise e

                    delay = context.calculate_delay()
                    context.total_delay += delay

                    logger.warning(
                        f"Operation {op_name} failed (attempt {context.attempt}), "
                        f"retrying in {delay:.2f}s: {e}"
                    )

                    time.sleep(delay)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            """Wrapper for asynchronous functions."""
            op_name = operation_name or func.__name__
            context = RetryContext(op_name, config)

            while True:
                try:
                    result = await func(*args, **kwargs)

                    if context.attempt > 0:
                        logger.info(
                            f"Operation {op_name} succeeded after {context.attempt} retries"
                        )

                    return result

                except Exception as e:
                    context.record_attempt(e)

                    if not context.should_retry(e):
                        logger.error(
                            f"Operation {op_name} failed after {context.attempt} attempts: {e}"
                        )
                        # Re-raise the last error
                        raise e

                    delay = context.calculate_delay()
                    context.total_delay += delay

                    logger.warning(
                        f"Operation {op_name} failed (attempt {context.attempt}), "
                        f"retrying in {delay:.2f}s: {e}"
                    )

                    await asyncio.sleep(delay)

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


class RetryableOperation:
    """
    Context manager for retryable operations with detailed error handling.

    Provides more control over retry logic than the decorator approach.
    """

    def __init__(
        self,
        operation_name: str,
        config: Optional[RetryConfig] = None,
        progress_reporter: Optional[Any] = None,
    ):
        self.operation_name = operation_name
        self.config = config or RetryConfig()
        self.progress_reporter = progress_reporter
        self.context = RetryContext(operation_name, self.config)

    async def execute_async(self, operation: Callable[[], Any]) -> Any:
        """Execute an async operation with retry logic."""
        while True:
            try:
                result = await operation()

                if self.context.attempt > 0:
                    logger.info(
                        f"Operation {self.operation_name} succeeded after {self.context.attempt} retries"
                    )
                    if self.progress_reporter:
                        self.progress_reporter.log_info(
                            f"Operation succeeded after {self.context.attempt} retries"
                        )

                return result

            except Exception as e:
                self.context.record_attempt(e)

                if not self.context.should_retry(e):
                    logger.error(
                        f"Operation {self.operation_name} failed permanently: {e}"
                    )
                    if self.progress_reporter:
                        self.progress_reporter.report_error(
                            f"Operation failed after {self.context.attempt} attempts",
                            context={"error": str(e), "attempts": self.context.attempt},
                        )
                    raise e

                delay = self.context.calculate_delay()
                self.context.total_delay += delay

                logger.warning(
                    f"Operation {self.operation_name} failed (attempt {self.context.attempt}), "
                    f"retrying in {delay:.2f}s: {e}"
                )

                if self.progress_reporter:
                    self.progress_reporter.log_debug(
                        f"Retrying in {delay:.2f}s (attempt {self.context.attempt})",
                        context={"error": str(e)},
                    )

                await asyncio.sleep(delay)

    def execute_sync(self, operation: Callable[[], Any]) -> Any:
        """Execute a sync operation with retry logic."""
        while True:
            try:
                result = operation()

                if self.context.attempt > 0:
                    logger.info(
                        f"Operation {self.operation_name} succeeded after {self.context.attempt} retries"
                    )
                    if self.progress_reporter:
                        self.progress_reporter.log_info(
                            f"Operation succeeded after {self.context.attempt} retries"
                        )

                return result

            except Exception as e:
                self.context.record_attempt(e)

                if not self.context.should_retry(e):
                    logger.error(
                        f"Operation {self.operation_name} failed permanently: {e}"
                    )
                    if self.progress_reporter:
                        self.progress_reporter.report_error(
                            f"Operation failed after {self.context.attempt} attempts",
                            context={"error": str(e), "attempts": self.context.attempt},
                        )
                    raise e

                delay = self.context.calculate_delay()
                self.context.total_delay += delay

                logger.warning(
                    f"Operation {self.operation_name} failed (attempt {self.context.attempt}), "
                    f"retrying in {delay:.2f}s: {e}"
                )

                if self.progress_reporter:
                    self.progress_reporter.log_debug(
                        f"Retrying in {delay:.2f}s (attempt {self.context.attempt})",
                        context={"error": str(e)},
                    )

                time.sleep(delay)


# Predefined retry configurations for common scenarios

# Configuration for backend API calls
BACKEND_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=30.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    retryable_error_categories=[
        ErrorCategory.CONNECTIVITY,
        ErrorCategory.SYSTEM,
    ],
)

# Configuration for database operations
DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=1.0,
    max_delay=60.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    retryable_error_categories=[
        ErrorCategory.DATABASE,
        ErrorCategory.CONNECTIVITY,
    ],
)

# Configuration for sync operations
SYNC_RETRY_CONFIG = RetryConfig(
    max_attempts=2,
    base_delay=5.0,
    max_delay=120.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    retryable_error_categories=[
        ErrorCategory.CONNECTIVITY,
        ErrorCategory.DATABASE,
    ],
)

# Configuration for file operations
FILE_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=10.0,
    backoff_strategy=BackoffStrategy.LINEAR,
    retryable_error_categories=[
        ErrorCategory.SYSTEM,
    ],
)


# Convenience functions for common retry patterns


def retry_backend_operation(operation_name: str = "backend_operation"):
    """Decorator for backend operations with appropriate retry config."""
    return retry_on_failure(BACKEND_RETRY_CONFIG, operation_name)


def retry_database_operation(operation_name: str = "database_operation"):
    """Decorator for database operations with appropriate retry config."""
    return retry_on_failure(DATABASE_RETRY_CONFIG, operation_name)


def retry_sync_operation(operation_name: str = "sync_operation"):
    """Decorator for sync operations with appropriate retry config."""
    return retry_on_failure(SYNC_RETRY_CONFIG, operation_name)


def retry_file_operation(operation_name: str = "file_operation"):
    """Decorator for file operations with appropriate retry config."""
    return retry_on_failure(FILE_RETRY_CONFIG, operation_name)
