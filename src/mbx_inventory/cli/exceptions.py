"""
Comprehensive error handling for inventory CLI operations.

This module provides a hierarchical exception system with detailed error reporting,
retry logic, and troubleshooting suggestions for different types of failures.
"""

import logging
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime


logger = logging.getLogger(__name__)


class ErrorSeverity(Enum):
    """Severity levels for errors."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of errors for better classification."""

    CONFIGURATION = "configuration"
    CONNECTIVITY = "connectivity"
    AUTHENTICATION = "authentication"
    DATA_VALIDATION = "data_validation"
    DATABASE = "database"
    SYNC_OPERATION = "sync_operation"
    SYSTEM = "system"


@dataclass
class ErrorContext:
    """Context information for errors."""

    operation: str
    table_name: Optional[str] = None
    record_count: Optional[int] = None
    batch_number: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.now)
    additional_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TroubleshootingSuggestion:
    """Troubleshooting suggestion for an error."""

    title: str
    description: str
    action_items: List[str]
    documentation_links: List[str] = field(default_factory=list)


class InventoryCLIError(Exception):
    """
    Base exception for inventory CLI operations.

    Provides comprehensive error information including context,
    severity, troubleshooting suggestions, and retry capabilities.
    """

    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
        suggestions: Optional[List[TroubleshootingSuggestion]] = None,
        retryable: bool = False,
        error_code: Optional[str] = None,
    ):
        """
        Initialize inventory CLI error.

        Args:
            message: Human-readable error message
            category: Error category for classification
            severity: Error severity level
            context: Additional context information
            cause: Original exception that caused this error
            suggestions: List of troubleshooting suggestions
            retryable: Whether this error can be retried
            error_code: Unique error code for documentation lookup
        """
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.context = context or ErrorContext(operation="unknown")
        self.cause = cause
        self.suggestions = suggestions or []
        self.retryable = retryable
        self.error_code = error_code
        self.timestamp = datetime.now()

        # Log the error
        self._log_error()

    def _log_error(self):
        """Log the error with appropriate level based on severity."""
        log_message = f"[{self.category.value.upper()}] {self.message}"

        if self.context:
            log_message += f" (Operation: {self.context.operation}"
            if self.context.table_name:
                log_message += f", Table: {self.context.table_name}"
            log_message += ")"

        if self.severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message, exc_info=self.cause)
        elif self.severity == ErrorSeverity.HIGH:
            logger.error(log_message, exc_info=self.cause)
        elif self.severity == ErrorSeverity.MEDIUM:
            logger.warning(log_message)
        else:
            logger.info(log_message)

    def get_formatted_message(self) -> str:
        """Get a formatted error message with context and suggestions."""
        lines = [f"Error: {self.message}"]

        if self.error_code:
            lines.append(f"Error Code: {self.error_code}")

        lines.append(f"Category: {self.category.value}")
        lines.append(f"Severity: {self.severity.value}")

        if self.context:
            lines.append(f"Operation: {self.context.operation}")
            if self.context.table_name:
                lines.append(f"Table: {self.context.table_name}")
            if self.context.record_count:
                lines.append(f"Records: {self.context.record_count}")

        if self.cause:
            lines.append(f"Underlying cause: {str(self.cause)}")

        if self.suggestions:
            lines.append("\nTroubleshooting suggestions:")
            for i, suggestion in enumerate(self.suggestions, 1):
                lines.append(f"{i}. {suggestion.title}")
                lines.append(f"   {suggestion.description}")
                for action in suggestion.action_items:
                    lines.append(f"   - {action}")

        return "\n".join(lines)


class ConfigurationError(InventoryCLIError):
    """Configuration file or validation errors."""

    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        missing_fields: Optional[List[str]] = None,
        invalid_values: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        suggestions = [
            TroubleshootingSuggestion(
                title="Check configuration file format",
                description="Ensure the configuration file is valid JSON with all required fields",
                action_items=[
                    "Validate JSON syntax using a JSON validator",
                    "Check that all required fields are present",
                    "Verify environment variables are set correctly",
                ],
                documentation_links=["https://docs.example.com/config"],
            )
        ]

        if missing_fields:
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Add missing configuration fields",
                    description=f"The following required fields are missing: {', '.join(missing_fields)}",
                    action_items=[
                        f"Add '{field}' to your configuration"
                        for field in missing_fields
                    ],
                )
            )

        if invalid_values:
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Fix invalid configuration values",
                    description="Some configuration values are invalid",
                    action_items=[
                        f"Fix '{field}': {error}"
                        for field, error in invalid_values.items()
                    ],
                )
            )

        super().__init__(
            message=message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            suggestions=suggestions,
            retryable=False,
            error_code="CONFIG_001",
            **kwargs,
        )

        self.config_file = config_file
        self.missing_fields = missing_fields or []
        self.invalid_values = invalid_values or {}


class BackendConnectionError(InventoryCLIError):
    """Backend connectivity and authentication errors."""

    def __init__(
        self,
        message: str,
        backend_type: Optional[str] = None,
        endpoint: Optional[str] = None,
        status_code: Optional[int] = None,
        **kwargs,
    ):
        suggestions = [
            TroubleshootingSuggestion(
                title="Check network connectivity",
                description="Verify that you can reach the backend service",
                action_items=[
                    "Test internet connectivity",
                    "Check if the backend service is accessible",
                    "Verify firewall settings",
                ],
            ),
            TroubleshootingSuggestion(
                title="Verify authentication credentials",
                description="Ensure API keys and authentication details are correct",
                action_items=[
                    "Check API key validity",
                    "Verify authentication token hasn't expired",
                    "Confirm user permissions for the backend",
                ],
            ),
        ]

        if status_code:
            if status_code == 401:
                suggestions.insert(
                    0,
                    TroubleshootingSuggestion(
                        title="Authentication failed",
                        description="The API key or credentials are invalid",
                        action_items=[
                            "Verify API key is correct",
                            "Check if API key has expired",
                            "Ensure proper permissions are granted",
                        ],
                    ),
                )
            elif status_code == 403:
                suggestions.insert(
                    0,
                    TroubleshootingSuggestion(
                        title="Access forbidden",
                        description="You don't have permission to access this resource",
                        action_items=[
                            "Check user permissions in the backend",
                            "Verify API key has required scopes",
                            "Contact administrator for access",
                        ],
                    ),
                )
            elif status_code >= 500:
                suggestions.insert(
                    0,
                    TroubleshootingSuggestion(
                        title="Backend service error",
                        description="The backend service is experiencing issues",
                        action_items=[
                            "Wait and retry the operation",
                            "Check backend service status",
                            "Contact backend service support",
                        ],
                    ),
                )

        super().__init__(
            message=message,
            category=ErrorCategory.CONNECTIVITY,
            severity=ErrorSeverity.HIGH,
            suggestions=suggestions,
            retryable=status_code is None or status_code >= 500,
            error_code="BACKEND_001",
            **kwargs,
        )

        self.backend_type = backend_type
        self.endpoint = endpoint
        self.status_code = status_code


class DatabaseError(InventoryCLIError):
    """Database connection and operation errors."""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        table_name: Optional[str] = None,
        constraint_violation: Optional[str] = None,
        **kwargs,
    ):
        suggestions = [
            TroubleshootingSuggestion(
                title="Check database connectivity",
                description="Verify database connection parameters and network access",
                action_items=[
                    "Test database connection manually",
                    "Verify host, port, and database name",
                    "Check username and password",
                    "Ensure database server is running",
                ],
            )
        ]

        if constraint_violation:
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Resolve constraint violation",
                    description=f"Database constraint violated: {constraint_violation}",
                    action_items=[
                        "Check data integrity requirements",
                        "Verify foreign key relationships",
                        "Ensure unique constraints are satisfied",
                    ],
                )
            )

        if "permission" in message.lower() or "access" in message.lower():
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Check database permissions",
                    description="The database user may not have required permissions",
                    action_items=[
                        "Verify user has SELECT, INSERT, UPDATE permissions",
                        "Check schema access permissions",
                        "Contact database administrator",
                    ],
                )
            )

        super().__init__(
            message=message,
            category=ErrorCategory.DATABASE,
            severity=ErrorSeverity.HIGH,
            suggestions=suggestions,
            retryable="timeout" in message.lower() or "connection" in message.lower(),
            error_code="DB_001",
            **kwargs,
        )

        self.operation = operation
        self.table_name = table_name
        self.constraint_violation = constraint_violation


class DataValidationError(InventoryCLIError):
    """Data validation and transformation errors."""

    def __init__(
        self,
        message: str,
        table_name: Optional[str] = None,
        invalid_records: Optional[List[Dict[str, Any]]] = None,
        validation_errors: Optional[Dict[str, List[str]]] = None,
        **kwargs,
    ):
        suggestions = [
            TroubleshootingSuggestion(
                title="Check data format and types",
                description="Verify that data matches expected schema and types",
                action_items=[
                    "Review data types in source system",
                    "Check for missing required fields",
                    "Validate date and numeric formats",
                ],
            )
        ]

        if validation_errors:
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Fix validation errors",
                    description="Address specific validation issues",
                    action_items=[
                        f"Fix {field}: {', '.join(errors)}"
                        for field, errors in validation_errors.items()
                    ],
                )
            )

        super().__init__(
            message=message,
            category=ErrorCategory.DATA_VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            suggestions=suggestions,
            retryable=False,
            error_code="DATA_001",
            **kwargs,
        )

        self.table_name = table_name
        self.invalid_records = invalid_records or []
        self.validation_errors = validation_errors or {}


class SyncOperationError(InventoryCLIError):
    """Synchronization operation errors."""

    def __init__(
        self,
        message: str,
        operation: str,
        table_name: Optional[str] = None,
        records_processed: int = 0,
        records_failed: int = 0,
        partial_success: bool = False,
        **kwargs,
    ):
        suggestions = [
            TroubleshootingSuggestion(
                title="Review sync operation details",
                description="Check the sync operation logs for specific errors",
                action_items=[
                    "Enable verbose logging for detailed information",
                    "Check individual table sync results",
                    "Review data transformation errors",
                ],
            )
        ]

        if partial_success:
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Handle partial success",
                    description="Some records were processed successfully",
                    action_items=[
                        "Review which records failed",
                        "Fix data issues and retry failed records",
                        "Consider running sync for specific tables only",
                    ],
                )
            )

        super().__init__(
            message=message,
            category=ErrorCategory.SYNC_OPERATION,
            severity=ErrorSeverity.MEDIUM if partial_success else ErrorSeverity.HIGH,
            suggestions=suggestions,
            retryable=True,
            error_code="SYNC_001",
            **kwargs,
        )

        self.operation = operation
        self.table_name = table_name
        self.records_processed = records_processed
        self.records_failed = records_failed
        self.partial_success = partial_success


class SystemError(InventoryCLIError):
    """System-level errors (memory, disk, permissions, etc.)."""

    def __init__(self, message: str, system_resource: Optional[str] = None, **kwargs):
        suggestions = [
            TroubleshootingSuggestion(
                title="Check system resources",
                description="Verify system has adequate resources for the operation",
                action_items=[
                    "Check available memory",
                    "Verify disk space",
                    "Monitor CPU usage",
                    "Check file permissions",
                ],
            )
        ]

        if "memory" in message.lower():
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Address memory issues",
                    description="The system is running low on memory",
                    action_items=[
                        "Reduce batch size for sync operations",
                        "Close other applications",
                        "Consider upgrading system memory",
                    ],
                )
            )

        if "permission" in message.lower():
            suggestions.append(
                TroubleshootingSuggestion(
                    title="Fix permission issues",
                    description="The application doesn't have required permissions",
                    action_items=[
                        "Check file and directory permissions",
                        "Run with appropriate user privileges",
                        "Verify write access to log directories",
                    ],
                )
            )

        super().__init__(
            message=message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.HIGH,
            suggestions=suggestions,
            retryable=False,
            error_code="SYS_001",
            **kwargs,
        )

        self.system_resource = system_resource


# Convenience functions for creating common errors


def create_config_error(
    message: str,
    config_file: Optional[str] = None,
    missing_fields: Optional[List[str]] = None,
    context: Optional[ErrorContext] = None,
) -> ConfigurationError:
    """Create a configuration error with common troubleshooting suggestions."""
    return ConfigurationError(
        message=message,
        config_file=config_file,
        missing_fields=missing_fields,
        context=context,
    )


def create_backend_error(
    message: str,
    backend_type: Optional[str] = None,
    status_code: Optional[int] = None,
    context: Optional[ErrorContext] = None,
    cause: Optional[Exception] = None,
) -> BackendConnectionError:
    """Create a backend connection error with appropriate suggestions."""
    return BackendConnectionError(
        message=message,
        backend_type=backend_type,
        status_code=status_code,
        context=context,
        cause=cause,
    )


def create_database_error(
    message: str,
    operation: Optional[str] = None,
    table_name: Optional[str] = None,
    context: Optional[ErrorContext] = None,
    cause: Optional[Exception] = None,
) -> DatabaseError:
    """Create a database error with operation context."""
    return DatabaseError(
        message=message,
        operation=operation,
        table_name=table_name,
        context=context,
        cause=cause,
    )


def create_sync_error(
    message: str,
    operation: str,
    table_name: Optional[str] = None,
    records_processed: int = 0,
    records_failed: int = 0,
    partial_success: bool = False,
    context: Optional[ErrorContext] = None,
    cause: Optional[Exception] = None,
) -> SyncOperationError:
    """Create a sync operation error with detailed context."""
    return SyncOperationError(
        message=message,
        operation=operation,
        table_name=table_name,
        records_processed=records_processed,
        records_failed=records_failed,
        partial_success=partial_success,
        context=context,
        cause=cause,
    )
