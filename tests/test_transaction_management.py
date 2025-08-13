"""
Tests for transaction management and rollback capabilities.

This module tests the transaction management system, rollback mechanisms,
and error recovery for database operations.
"""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime

from mbx_inventory.cli.transaction_manager import (
    TransactionManager,
    SyncTransactionManager,
    TransactionContext,
    TransactionOperation,
    TransactionState,
    with_sync_transaction,
)
from mbx_inventory.cli.exceptions import DatabaseError, SyncOperationError
from mbx_inventory.cli.progress import ProgressReporter
from mbx_db.sync import UpsertResult


class TestTransactionContext:
    """Test transaction context functionality."""

    def test_transaction_context_creation(self):
        """Test creating a transaction context."""
        context = TransactionContext(transaction_id="test_tx_001")

        assert context.transaction_id == "test_tx_001"
        assert context.state == TransactionState.PENDING
        assert len(context.operations) == 0
        assert len(context.savepoints) == 0
        assert context.completed_at is None
        assert context.error_message is None

    def test_add_operation(self):
        """Test adding operations to transaction context."""
        context = TransactionContext(transaction_id="test_tx_001")

        operation = TransactionOperation(
            operation_id="op_001",
            table_name="elements",
            operation_type="upsert",
            record_count=50,
            success=True,
        )

        context.add_operation(operation)

        assert len(context.operations) == 1
        assert context.operations[0].operation_id == "op_001"
        assert context.operations[0].table_name == "elements"
        assert context.operations[0].success is True

    def test_get_successful_and_failed_operations(self):
        """Test filtering operations by success status."""
        context = TransactionContext(transaction_id="test_tx_001")

        # Add successful operation
        successful_op = TransactionOperation(
            operation_id="op_success",
            table_name="elements",
            operation_type="upsert",
            record_count=50,
            success=True,
        )
        context.add_operation(successful_op)

        # Add failed operation
        failed_op = TransactionOperation(
            operation_id="op_failed",
            table_name="stations",
            operation_type="upsert",
            record_count=25,
            success=False,
            error_message="Constraint violation",
        )
        context.add_operation(failed_op)

        successful_ops = context.get_successful_operations()
        failed_ops = context.get_failed_operations()

        assert len(successful_ops) == 1
        assert len(failed_ops) == 1
        assert successful_ops[0].operation_id == "op_success"
        assert failed_ops[0].operation_id == "op_failed"

    def test_transaction_summary(self):
        """Test transaction summary generation."""
        context = TransactionContext(transaction_id="test_tx_001")
        context.state = TransactionState.COMMITTED
        context.completed_at = datetime.now()

        # Add operations
        successful_op = TransactionOperation(
            operation_id="op_001",
            table_name="elements",
            operation_type="upsert",
            record_count=100,
            success=True,
        )
        context.add_operation(successful_op)

        failed_op = TransactionOperation(
            operation_id="op_002",
            table_name="stations",
            operation_type="upsert",
            record_count=50,
            success=False,
        )
        context.add_operation(failed_op)

        summary = context.get_summary()

        assert summary["transaction_id"] == "test_tx_001"
        assert summary["state"] == "committed"
        assert summary["total_operations"] == 2
        assert summary["successful_operations"] == 1
        assert summary["failed_operations"] == 1
        assert summary["total_records_processed"] == 100
        assert "elements" in summary["tables_affected"]
        assert "stations" in summary["tables_affected"]


class TestTransactionManager:
    """Test basic transaction manager functionality."""

    @pytest.fixture
    def mock_engine(self):
        """Create a mock SQLAlchemy engine."""
        engine = AsyncMock()
        return engine

    @pytest.fixture
    def mock_progress_reporter(self):
        """Create a mock progress reporter."""
        return Mock(spec=ProgressReporter)

    @pytest.fixture
    def transaction_manager(self, mock_engine, mock_progress_reporter):
        """Create a transaction manager instance."""
        return TransactionManager(
            engine=mock_engine,
            progress_reporter=mock_progress_reporter,
            enable_savepoints=True,
        )

    @pytest.mark.asyncio
    async def test_successful_transaction(self, transaction_manager, mock_engine):
        """Test successful transaction execution."""
        # Mock connection and transaction
        mock_connection = AsyncMock()
        mock_transaction = AsyncMock()

        # Set up the mock chain properly
        mock_engine.connect.return_value = mock_connection
        mock_connection.__aenter__.return_value = mock_connection
        mock_connection.__aexit__.return_value = None
        mock_connection.begin.return_value = mock_transaction
        mock_transaction.__aenter__.return_value = mock_transaction
        mock_transaction.__aexit__.return_value = None

        transaction_id = "test_tx_001"

        async with transaction_manager.transaction(transaction_id) as context:
            assert context.transaction_id == transaction_id
            assert context.state == TransactionState.ACTIVE

            # Simulate some operations
            operation = TransactionOperation(
                operation_id="op_001",
                table_name="elements",
                operation_type="upsert",
                record_count=50,
                success=True,
            )
            transaction_manager.record_operation(transaction_id, operation)

        # Verify transaction was committed
        mock_transaction.commit.assert_called_once()
        assert context.state == TransactionState.COMMITTED
        assert context.completed_at is not None

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(
        self, transaction_manager, mock_engine
    ):
        """Test transaction rollback when error occurs."""
        # Mock connection and transaction
        mock_connection = AsyncMock()
        mock_transaction = AsyncMock()

        # Set up the mock chain properly
        mock_engine.connect.return_value = mock_connection
        mock_connection.__aenter__.return_value = mock_connection
        mock_connection.__aexit__.return_value = None
        mock_connection.begin.return_value = mock_transaction
        mock_transaction.__aenter__.return_value = mock_transaction
        mock_transaction.__aexit__.return_value = None

        transaction_id = "test_tx_002"

        with pytest.raises(DatabaseError):
            async with transaction_manager.transaction(transaction_id) as context:
                assert context.state == TransactionState.ACTIVE

                # Simulate an error
                raise ValueError("Simulated database error")

        # Verify transaction was rolled back
        mock_transaction.rollback.assert_called_once()
        assert context.state == TransactionState.ROLLED_BACK
        assert context.error_message is not None

    @pytest.mark.asyncio
    async def test_savepoint_creation(self, transaction_manager):
        """Test savepoint creation and management."""
        mock_connection = AsyncMock()
        transaction_id = "test_tx_003"
        savepoint_name = "sp_test"

        # Add transaction to active transactions
        context = TransactionContext(transaction_id=transaction_id)
        transaction_manager.active_transactions[transaction_id] = context

        result = await transaction_manager.create_savepoint(
            mock_connection, transaction_id, savepoint_name
        )

        assert result is True
        mock_connection.execute.assert_called_once()
        assert savepoint_name in context.savepoints

    @pytest.mark.asyncio
    async def test_rollback_to_savepoint(
        self, transaction_manager, mock_progress_reporter
    ):
        """Test rolling back to a savepoint."""
        # Create a new transaction manager with the mock progress reporter
        transaction_manager.progress_reporter = mock_progress_reporter

        mock_connection = AsyncMock()
        transaction_id = "test_tx_004"
        savepoint_name = "sp_test"

        # Add transaction with savepoint
        context = TransactionContext(transaction_id=transaction_id)
        context.savepoints[savepoint_name] = datetime.now()
        transaction_manager.active_transactions[transaction_id] = context

        result = await transaction_manager.rollback_to_savepoint(
            mock_connection, transaction_id, savepoint_name
        )

        assert result is True
        mock_connection.execute.assert_called_once()
        # Verify the correct SQL was executed
        call_args = mock_connection.execute.call_args[0][0]
        assert f"ROLLBACK TO SAVEPOINT {savepoint_name}" in str(call_args)

    def test_record_operation(self, transaction_manager):
        """Test recording operations in transaction context."""
        transaction_id = "test_tx_005"

        # Add transaction to active transactions
        context = TransactionContext(transaction_id=transaction_id)
        transaction_manager.active_transactions[transaction_id] = context

        operation = TransactionOperation(
            operation_id="op_001",
            table_name="elements",
            operation_type="upsert",
            record_count=25,
            success=True,
        )

        transaction_manager.record_operation(transaction_id, operation)

        assert len(context.operations) == 1
        assert context.operations[0].operation_id == "op_001"

    def test_get_transaction_summary(self, transaction_manager):
        """Test getting transaction summary."""
        transaction_id = "test_tx_006"

        # Add transaction with operations
        context = TransactionContext(transaction_id=transaction_id)
        operation = TransactionOperation(
            operation_id="op_001",
            table_name="elements",
            operation_type="upsert",
            record_count=100,
            success=True,
        )
        context.add_operation(operation)
        transaction_manager.active_transactions[transaction_id] = context

        summary = transaction_manager.get_transaction_summary(transaction_id)

        assert summary is not None
        assert summary["transaction_id"] == transaction_id
        assert summary["total_operations"] == 1
        assert summary["successful_operations"] == 1


class TestSyncTransactionManager:
    """Test sync-specific transaction manager functionality."""

    @pytest.fixture
    def mock_engine(self):
        """Create a mock SQLAlchemy engine."""
        return AsyncMock()

    @pytest.fixture
    def mock_progress_reporter(self):
        """Create a mock progress reporter."""
        return Mock(spec=ProgressReporter)

    @pytest.fixture
    def sync_transaction_manager(self, mock_engine, mock_progress_reporter):
        """Create a sync transaction manager instance."""
        return SyncTransactionManager(
            engine=mock_engine,
            progress_reporter=mock_progress_reporter,
            schema="network",
        )

    @pytest.mark.asyncio
    async def test_execute_table_sync_with_transaction(self, sync_transaction_manager):
        """Test executing table sync within a transaction."""
        transaction_id = "sync_tx_001"
        table_name = "elements"

        # Create transaction context
        context = TransactionContext(transaction_id=transaction_id)
        sync_transaction_manager.active_transactions[transaction_id] = context

        # Mock sync operation
        mock_result = UpsertResult(
            table_name="network.elements",
            records_processed=100,
            records_created=50,
            records_updated=40,
            records_failed=10,
            errors=["Some error"],
        )

        async def mock_sync_operation():
            return mock_result

        result = await sync_transaction_manager.execute_table_sync_with_transaction(
            transaction_id=transaction_id,
            table_name=table_name,
            sync_operation=mock_sync_operation,
            create_savepoint=False,  # Disable savepoint for this test
        )

        assert result == mock_result
        assert len(context.operations) == 1

        operation = context.operations[0]
        assert operation.table_name == table_name
        assert operation.operation_type == "upsert"
        assert operation.record_count == 100
        assert operation.success is False  # Because records_failed > 0

    @pytest.mark.asyncio
    async def test_table_sync_with_error_and_rollback(self, sync_transaction_manager):
        """Test table sync with error and savepoint rollback."""
        transaction_id = "sync_tx_002"
        table_name = "stations"

        # Create transaction context
        context = TransactionContext(transaction_id=transaction_id)
        sync_transaction_manager.active_transactions[transaction_id] = context

        # Mock sync operation that fails
        async def failing_sync_operation():
            raise ValueError("Sync operation failed")

        with pytest.raises(SyncOperationError):
            await sync_transaction_manager.execute_table_sync_with_transaction(
                transaction_id=transaction_id,
                table_name=table_name,
                sync_operation=failing_sync_operation,
                create_savepoint=False,
            )

        # Verify operation was recorded as failed
        assert len(context.operations) == 1
        operation = context.operations[0]
        assert operation.success is False
        assert operation.error_message == "Sync operation failed"


class TestTransactionIntegration:
    """Test integration of transaction management with sync operations."""

    @pytest.mark.asyncio
    async def test_with_sync_transaction_helper(self):
        """Test the with_sync_transaction helper function."""
        mock_engine = AsyncMock()
        mock_connection = AsyncMock()
        mock_transaction = AsyncMock()

        # Set up the mock chain properly
        mock_engine.connect.return_value = mock_connection
        mock_connection.__aenter__.return_value = mock_connection
        mock_connection.__aexit__.return_value = None
        mock_connection.begin.return_value = mock_transaction
        mock_transaction.__aenter__.return_value = mock_transaction
        mock_transaction.__aexit__.return_value = None

        transaction_id = "helper_tx_001"

        async def mock_sync_operations(tx_context, tx_manager):
            assert tx_context.transaction_id == transaction_id
            assert isinstance(tx_manager, SyncTransactionManager)
            return "sync_result"

        result = await with_sync_transaction(
            engine=mock_engine,
            transaction_id=transaction_id,
            sync_operations=mock_sync_operations,
            isolation_level="READ_COMMITTED",
        )

        assert result == "sync_result"
        mock_transaction.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_transaction_isolation_level(self):
        """Test setting transaction isolation level."""
        mock_engine = AsyncMock()
        mock_connection = AsyncMock()
        mock_transaction = AsyncMock()

        # Set up the mock chain properly
        mock_engine.connect.return_value = mock_connection
        mock_connection.__aenter__.return_value = mock_connection
        mock_connection.__aexit__.return_value = None
        mock_connection.begin.return_value = mock_transaction
        mock_transaction.__aenter__.return_value = mock_transaction
        mock_transaction.__aexit__.return_value = None

        transaction_manager = TransactionManager(mock_engine)

        async with transaction_manager.transaction(
            transaction_id="iso_tx_001", isolation_level="SERIALIZABLE"
        ):
            pass

        # Verify isolation level was set
        mock_connection.execute.assert_called()
        call_args = mock_connection.execute.call_args[0][0]
        assert "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" in str(call_args)

    @pytest.mark.asyncio
    async def test_transaction_recovery_mechanisms(self):
        """Test transaction recovery mechanisms."""
        mock_engine = AsyncMock()
        mock_progress_reporter = Mock(spec=ProgressReporter)
        transaction_manager = TransactionManager(mock_engine, mock_progress_reporter)

        transaction_id = "recovery_tx_001"

        # Create failed operations
        failed_operations = [
            TransactionOperation(
                operation_id="op_001",
                table_name="elements",
                operation_type="upsert",
                record_count=50,
                success=False,
                error_message="Constraint violation",
            ),
            TransactionOperation(
                operation_id="op_002",
                table_name="stations",
                operation_type="upsert",
                record_count=25,
                success=False,
                error_message="Timeout error",
            ),
        ]

        # Test rollback_failed_tables strategy
        result = await transaction_manager.recover_from_partial_failure(
            transaction_id=transaction_id,
            failed_operations=failed_operations,
            recovery_strategy="rollback_failed_tables",
        )

        assert result is True

        # Test retry_failed strategy
        result = await transaction_manager.recover_from_partial_failure(
            transaction_id=transaction_id,
            failed_operations=failed_operations,
            recovery_strategy="retry_failed",
        )

        assert result is True

        # Test manual recovery
        result = await transaction_manager.recover_from_partial_failure(
            transaction_id=transaction_id,
            failed_operations=failed_operations,
            recovery_strategy="manual",
        )

        assert result is False


if __name__ == "__main__":
    pytest.main([__file__])
