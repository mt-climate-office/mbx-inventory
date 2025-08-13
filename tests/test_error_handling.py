"""
Tests for comprehensive error handling in inventory CLI.

This module tests the error hierarchy, retry logic, and error recovery mechanisms.
"""

import pytest
from unittest.mock import patch

from mbx_inventory.cli.exceptions import (
    InventoryCLIError,
    ConfigurationError,
    BackendConnectionError,
    DatabaseError,
    SyncOperationError,
    ErrorCategory,
    ErrorSeverity,
    ErrorContext,
    TroubleshootingSuggestion,
    create_config_error,
    create_backend_error,
    create_database_error,
    create_sync_error,
)
from mbx_inventory.cli.retry import (
    RetryConfig,
    RetryContext,
    BackoffStrategy,
    retry_on_failure,
    RetryableOperation,
    BACKEND_RETRY_CONFIG,
    DATABASE_RETRY_CONFIG,
)


class TestErrorHierarchy:
    """Test the error hierarchy and error creation functions."""

    def test_base_inventory_cli_error(self):
        """Test base InventoryCLIError functionality."""
        context = ErrorContext(operation="test_operation", table_name="test_table")
        suggestion = TroubleshootingSuggestion(
            title="Test suggestion",
            description="This is a test suggestion",
            action_items=["Do this", "Do that"],
        )

        error = InventoryCLIError(
            message="Test error message",
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            context=context,
            suggestions=[suggestion],
            retryable=True,
            error_code="TEST_001",
        )

        assert error.message == "Test error message"
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH
        assert error.context.operation == "test_operation"
        assert error.context.table_name == "test_table"
        assert error.retryable is True
        assert error.error_code == "TEST_001"
        assert len(error.suggestions) == 1
        assert error.suggestions[0].title == "Test suggestion"

    def test_configuration_error(self):
        """Test ConfigurationError with missing fields."""
        error = create_config_error(
            message="Configuration validation failed",
            config_file="test_config.json",
            missing_fields=["api_key", "base_id"],
        )

        assert isinstance(error, ConfigurationError)
        assert error.category == ErrorCategory.CONFIGURATION
        assert error.severity == ErrorSeverity.HIGH
        assert error.config_file == "test_config.json"
        assert "api_key" in error.missing_fields
        assert "base_id" in error.missing_fields
        assert error.error_code == "CONFIG_001"
        assert not error.retryable

    def test_backend_connection_error(self):
        """Test BackendConnectionError with status codes."""
        # Test 401 Unauthorized
        error = create_backend_error(
            message="Authentication failed", backend_type="airtable", status_code=401
        )

        assert isinstance(error, BackendConnectionError)
        assert error.category == ErrorCategory.CONNECTIVITY
        assert error.backend_type == "airtable"
        assert error.status_code == 401
        assert error.retryable is False  # 401 is not retryable

        # Test 500 Server Error (retryable)
        error = create_backend_error(
            message="Server error", backend_type="baserow", status_code=500
        )

        assert error.retryable is True  # 500 is retryable

    def test_database_error(self):
        """Test DatabaseError with operation context."""
        context = ErrorContext(operation="sync_table", table_name="elements")

        error = create_database_error(
            message="Connection timeout",
            operation="insert",
            table_name="elements",
            context=context,
        )

        assert isinstance(error, DatabaseError)
        assert error.category == ErrorCategory.DATABASE
        assert error.operation == "insert"
        assert error.table_name == "elements"
        assert error.retryable is True  # timeout is retryable

    def test_sync_operation_error(self):
        """Test SyncOperationError with partial success."""
        context = ErrorContext(operation="sync_all_tables")

        error = create_sync_error(
            message="Sync partially failed",
            operation="sync_all_tables",
            records_processed=100,
            records_failed=10,
            partial_success=True,
            context=context,
        )

        assert isinstance(error, SyncOperationError)
        assert error.category == ErrorCategory.SYNC_OPERATION
        assert error.operation == "sync_all_tables"
        assert error.records_processed == 100
        assert error.records_failed == 10
        assert error.partial_success is True
        assert (
            error.severity == ErrorSeverity.MEDIUM
        )  # partial success = medium severity

    def test_error_formatted_message(self):
        """Test formatted error message generation."""
        context = ErrorContext(
            operation="test_operation", table_name="test_table", record_count=50
        )

        suggestion = TroubleshootingSuggestion(
            title="Check configuration",
            description="Verify your configuration file",
            action_items=["Check JSON syntax", "Verify environment variables"],
        )

        error = InventoryCLIError(
            message="Test error",
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            context=context,
            suggestions=[suggestion],
            error_code="TEST_001",
        )

        formatted = error.get_formatted_message()

        assert "Test error" in formatted
        assert "TEST_001" in formatted
        assert "configuration" in formatted
        assert "high" in formatted
        assert "test_operation" in formatted
        assert "test_table" in formatted
        assert "Records: 50" in formatted
        assert "Check configuration" in formatted
        assert "Check JSON syntax" in formatted


class TestRetryLogic:
    """Test retry logic and backoff strategies."""

    def test_retry_config(self):
        """Test retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            max_delay=30.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
        )

        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 30.0
        assert config.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER

    def test_retry_context(self):
        """Test retry context functionality."""
        config = RetryConfig(max_attempts=3)
        context = RetryContext("test_operation", config)

        # Test retryable exceptions
        assert context.should_retry(ConnectionError("Network error"))
        assert context.should_retry(TimeoutError("Operation timeout"))

        # Test non-retryable exceptions
        assert not context.should_retry(ValueError("Invalid value"))

        # Test InventoryCLIError with retryable flag
        retryable_error = InventoryCLIError(
            message="Retryable error",
            category=ErrorCategory.CONNECTIVITY,
            retryable=True,
        )
        assert context.should_retry(retryable_error)

        non_retryable_error = InventoryCLIError(
            message="Non-retryable error",
            category=ErrorCategory.CONFIGURATION,
            retryable=False,
        )
        assert not context.should_retry(non_retryable_error)

    def test_backoff_calculation(self):
        """Test different backoff strategies."""
        config = RetryConfig(base_delay=1.0, max_delay=10.0)
        context = RetryContext("test", config)

        # Test fixed backoff
        config.backoff_strategy = BackoffStrategy.FIXED
        context.attempt = 1
        assert context.calculate_delay() == 1.0
        context.attempt = 3
        assert context.calculate_delay() == 1.0

        # Test linear backoff
        config.backoff_strategy = BackoffStrategy.LINEAR
        context.attempt = 1
        assert context.calculate_delay() == 1.0
        context.attempt = 3
        assert context.calculate_delay() == 3.0

        # Test exponential backoff
        config.backoff_strategy = BackoffStrategy.EXPONENTIAL
        context.attempt = 1
        assert context.calculate_delay() == 1.0
        context.attempt = 2
        assert context.calculate_delay() == 2.0
        context.attempt = 3
        assert context.calculate_delay() == 4.0

        # Test max delay cap
        context.attempt = 10
        delay = context.calculate_delay()
        assert delay <= config.max_delay

    def test_retry_decorator_sync(self):
        """Test retry decorator for synchronous functions."""
        call_count = 0

        @retry_on_failure(RetryConfig(max_attempts=3, base_delay=0.1))
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "success"

        result = failing_function()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_decorator_async(self):
        """Test retry decorator for asynchronous functions."""
        call_count = 0

        @retry_on_failure(RetryConfig(max_attempts=3, base_delay=0.1))
        async def failing_async_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network error")
            return "async_success"

        result = await failing_async_function()
        assert result == "async_success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retryable_operation(self):
        """Test RetryableOperation context manager."""
        call_count = 0

        async def failing_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network error")
            return "operation_success"

        config = RetryConfig(max_attempts=3, base_delay=0.1)
        retry_op = RetryableOperation("test_operation", config)

        result = await retry_op.execute_async(failing_operation)
        assert result == "operation_success"
        assert call_count == 2

    def test_predefined_retry_configs(self):
        """Test predefined retry configurations."""
        # Test backend retry config
        assert BACKEND_RETRY_CONFIG.max_attempts == 3
        assert BACKEND_RETRY_CONFIG.base_delay == 2.0
        assert (
            ErrorCategory.CONNECTIVITY
            in BACKEND_RETRY_CONFIG.retryable_error_categories
        )

        # Test database retry config
        assert DATABASE_RETRY_CONFIG.max_attempts == 5
        assert DATABASE_RETRY_CONFIG.base_delay == 1.0
        assert (
            ErrorCategory.DATABASE in DATABASE_RETRY_CONFIG.retryable_error_categories
        )


class TestErrorRecovery:
    """Test error recovery mechanisms."""

    def test_partial_success_handling(self):
        """Test handling of partial success scenarios."""
        # Create a sync error with partial success
        error = create_sync_error(
            message="Some tables failed to sync",
            operation="sync_all_tables",
            records_processed=1000,
            records_failed=100,
            partial_success=True,
        )

        assert error.partial_success is True
        assert error.severity == ErrorSeverity.MEDIUM
        assert error.retryable is True

        # Check that suggestions include partial success handling
        formatted = error.get_formatted_message()
        assert "partial success" in formatted.lower()

    def test_error_context_preservation(self):
        """Test that error context is preserved through error chain."""
        original_context = ErrorContext(
            operation="sync_table",
            table_name="elements",
            record_count=100,
            additional_data={"batch_number": 1},
        )

        original_error = ValueError("Original error")

        wrapped_error = create_sync_error(
            message="Wrapped error",
            operation="sync_table",
            table_name="elements",
            context=original_context,
            cause=original_error,
        )

        assert wrapped_error.context.operation == "sync_table"
        assert wrapped_error.context.table_name == "elements"
        assert wrapped_error.context.record_count == 100
        assert wrapped_error.context.additional_data["batch_number"] == 1
        assert wrapped_error.cause == original_error

    @pytest.mark.asyncio
    async def test_error_logging(self):
        """Test that errors are properly logged."""
        with patch("mbx_inventory.cli.exceptions.logger") as mock_logger:
            error = InventoryCLIError(
                message="Test error",
                category=ErrorCategory.CONFIGURATION,
                severity=ErrorSeverity.CRITICAL,
            )

            # Check that critical error was logged
            mock_logger.critical.assert_called_once()

            # Test different severity levels
            error = InventoryCLIError(
                message="High severity error",
                category=ErrorCategory.DATABASE,
                severity=ErrorSeverity.HIGH,
            )
            mock_logger.error.assert_called()

            error = InventoryCLIError(
                message="Medium severity error",
                category=ErrorCategory.SYNC_OPERATION,
                severity=ErrorSeverity.MEDIUM,
            )
            mock_logger.warning.assert_called()

            error = InventoryCLIError(
                message="Low severity error",
                category=ErrorCategory.DATA_VALIDATION,
                severity=ErrorSeverity.LOW,
            )
            mock_logger.info.assert_called()


if __name__ == "__main__":
    pytest.main([__file__])
