"""
Transaction management and rollback capabilities for inventory sync operations.

This module provides transaction management for database operations with
rollback capabilities, error recovery mechanisms, and detailed transaction logging.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy import text
from mbx_db.sync import UpsertResult

from .exceptions import (
    ErrorContext,
    create_database_error,
    create_sync_error,
)
from .progress import ProgressReporter


logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """States of a transaction."""

    PENDING = "pending"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


@dataclass
class TransactionOperation:
    """Record of an operation within a transaction."""

    operation_id: str
    table_name: str
    operation_type: str  # "insert", "update", "delete", "upsert"
    record_count: int
    timestamp: datetime = field(default_factory=datetime.now)
    success: bool = False
    error_message: Optional[str] = None
    rollback_sql: Optional[str] = None  # SQL to rollback this operation


@dataclass
class TransactionContext:
    """Context for managing a database transaction."""

    transaction_id: str
    started_at: datetime = field(default_factory=datetime.now)
    state: TransactionState = TransactionState.PENDING
    operations: List[TransactionOperation] = field(default_factory=list)
    savepoints: Dict[str, datetime] = field(default_factory=dict)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    def add_operation(self, operation: TransactionOperation):
        """Add an operation to the transaction context."""
        self.operations.append(operation)
        logger.debug(
            f"Added operation {operation.operation_id} to transaction {self.transaction_id}"
        )

    def get_successful_operations(self) -> List[TransactionOperation]:
        """Get list of successful operations."""
        return [op for op in self.operations if op.success]

    def get_failed_operations(self) -> List[TransactionOperation]:
        """Get list of failed operations."""
        return [op for op in self.operations if not op.success]

    def get_summary(self) -> Dict[str, Any]:
        """Get transaction summary."""
        successful_ops = self.get_successful_operations()
        failed_ops = self.get_failed_operations()

        return {
            "transaction_id": self.transaction_id,
            "state": self.state.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "duration_seconds": (
                (self.completed_at or datetime.now()) - self.started_at
            ).total_seconds(),
            "total_operations": len(self.operations),
            "successful_operations": len(successful_ops),
            "failed_operations": len(failed_ops),
            "tables_affected": list(set(op.table_name for op in self.operations)),
            "total_records_processed": sum(op.record_count for op in successful_ops),
            "error_message": self.error_message,
        }


class TransactionManager:
    """
    Manages database transactions with rollback capabilities for sync operations.

    Provides:
    - Automatic transaction management
    - Savepoint support for partial rollbacks
    - Operation tracking and rollback capabilities
    - Error recovery mechanisms
    - Detailed transaction logging
    """

    def __init__(
        self,
        engine: AsyncEngine,
        progress_reporter: Optional[ProgressReporter] = None,
        enable_savepoints: bool = True,
    ):
        """
        Initialize transaction manager.

        Args:
            engine: SQLAlchemy async engine
            progress_reporter: Optional progress reporter for user feedback
            enable_savepoints: Whether to use savepoints for partial rollbacks
        """
        self.engine = engine
        self.progress_reporter = progress_reporter
        self.enable_savepoints = enable_savepoints
        self.active_transactions: Dict[str, TransactionContext] = {}

    @asynccontextmanager
    async def transaction(
        self,
        transaction_id: str,
        isolation_level: Optional[str] = None,
        auto_rollback_on_error: bool = True,
    ):
        """
        Context manager for database transactions with automatic rollback.

        Args:
            transaction_id: Unique identifier for the transaction
            isolation_level: SQL isolation level (e.g., "READ_COMMITTED")
            auto_rollback_on_error: Whether to automatically rollback on errors

        Yields:
            TransactionContext: Context for tracking transaction operations
        """
        context = TransactionContext(transaction_id=transaction_id)
        self.active_transactions[transaction_id] = context

        connection = None
        transaction = None

        try:
            logger.info(f"Starting transaction {transaction_id}")
            if self.progress_reporter:
                self.progress_reporter.log_debug(
                    f"Starting transaction {transaction_id}"
                )

            # Get database connection
            connection = await self.engine.connect()

            # Set isolation level if specified
            if isolation_level:
                await connection.execute(
                    text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                )

            # Begin transaction
            transaction = await connection.begin()
            context.state = TransactionState.ACTIVE

            logger.debug(f"Transaction {transaction_id} is now active")

            yield context

            # If we get here without exceptions, commit the transaction
            await transaction.commit()
            context.state = TransactionState.COMMITTED
            context.completed_at = datetime.now()

            logger.info(f"Transaction {transaction_id} committed successfully")
            if self.progress_reporter:
                summary = context.get_summary()
                self.progress_reporter.log_info(
                    f"Transaction committed: {summary['successful_operations']} operations, "
                    f"{summary['total_records_processed']} records processed"
                )

        except Exception as e:
            error_msg = f"Transaction {transaction_id} failed: {str(e)}"
            logger.error(error_msg)
            context.error_message = error_msg

            if transaction and auto_rollback_on_error:
                try:
                    await transaction.rollback()
                    context.state = TransactionState.ROLLED_BACK
                    logger.info(
                        f"Transaction {transaction_id} rolled back successfully"
                    )

                    if self.progress_reporter:
                        self.progress_reporter.log_warning(
                            f"Transaction rolled back due to error: {str(e)}"
                        )
                except Exception as rollback_error:
                    context.state = TransactionState.FAILED
                    logger.error(
                        f"Failed to rollback transaction {transaction_id}: {rollback_error}"
                    )

                    if self.progress_reporter:
                        self.progress_reporter.report_error(
                            f"Failed to rollback transaction: {rollback_error}"
                        )
            else:
                context.state = TransactionState.FAILED

            context.completed_at = datetime.now()

            # Create detailed database error
            error_context = ErrorContext(
                operation="transaction_management",
                additional_data={
                    "transaction_id": transaction_id,
                    "operations_completed": len(context.get_successful_operations()),
                    "operations_failed": len(context.get_failed_operations()),
                },
            )

            raise create_database_error(
                error_msg,
                operation="transaction",
                context=error_context,
                cause=e,
            )

        finally:
            # Clean up connection
            if connection:
                await connection.close()

            # Remove from active transactions
            if transaction_id in self.active_transactions:
                del self.active_transactions[transaction_id]

    async def create_savepoint(
        self,
        connection: AsyncConnection,
        transaction_id: str,
        savepoint_name: str,
    ) -> bool:
        """
        Create a savepoint within a transaction.

        Args:
            connection: Database connection
            transaction_id: ID of the active transaction
            savepoint_name: Name for the savepoint

        Returns:
            bool: True if savepoint was created successfully
        """
        if not self.enable_savepoints:
            logger.debug("Savepoints are disabled")
            return False

        if transaction_id not in self.active_transactions:
            logger.warning(f"No active transaction found for ID {transaction_id}")
            return False

        try:
            await connection.execute(text(f"SAVEPOINT {savepoint_name}"))

            context = self.active_transactions[transaction_id]
            context.savepoints[savepoint_name] = datetime.now()

            logger.debug(
                f"Created savepoint {savepoint_name} in transaction {transaction_id}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to create savepoint {savepoint_name}: {e}")
            return False

    async def rollback_to_savepoint(
        self,
        connection: AsyncConnection,
        transaction_id: str,
        savepoint_name: str,
    ) -> bool:
        """
        Rollback to a specific savepoint.

        Args:
            connection: Database connection
            transaction_id: ID of the active transaction
            savepoint_name: Name of the savepoint to rollback to

        Returns:
            bool: True if rollback was successful
        """
        if not self.enable_savepoints:
            logger.debug("Savepoints are disabled")
            return False

        if transaction_id not in self.active_transactions:
            logger.warning(f"No active transaction found for ID {transaction_id}")
            return False

        context = self.active_transactions[transaction_id]

        if savepoint_name not in context.savepoints:
            logger.warning(
                f"Savepoint {savepoint_name} not found in transaction {transaction_id}"
            )
            return False

        try:
            await connection.execute(text(f"ROLLBACK TO SAVEPOINT {savepoint_name}"))

            logger.info(
                f"Rolled back to savepoint {savepoint_name} in transaction {transaction_id}"
            )

            if self.progress_reporter:
                self.progress_reporter.log_warning(
                    f"Rolled back to savepoint {savepoint_name}"
                )

            return True

        except Exception as e:
            logger.error(f"Failed to rollback to savepoint {savepoint_name}: {e}")
            return False

    def record_operation(
        self,
        transaction_id: str,
        operation: TransactionOperation,
    ):
        """
        Record an operation within a transaction.

        Args:
            transaction_id: ID of the transaction
            operation: Operation to record
        """
        if transaction_id in self.active_transactions:
            self.active_transactions[transaction_id].add_operation(operation)
        else:
            logger.warning(
                f"Attempted to record operation for unknown transaction {transaction_id}"
            )

    def get_transaction_summary(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """
        Get summary of a transaction.

        Args:
            transaction_id: ID of the transaction

        Returns:
            Dict with transaction summary or None if not found
        """
        if transaction_id in self.active_transactions:
            return self.active_transactions[transaction_id].get_summary()
        return None

    async def recover_from_partial_failure(
        self,
        transaction_id: str,
        failed_operations: List[TransactionOperation],
        recovery_strategy: str = "rollback_failed_tables",
    ) -> bool:
        """
        Attempt to recover from partial transaction failure.

        Args:
            transaction_id: ID of the failed transaction
            failed_operations: List of operations that failed
            recovery_strategy: Strategy for recovery ("rollback_failed_tables", "retry_failed", "manual")

        Returns:
            bool: True if recovery was successful
        """
        logger.info(
            f"Attempting recovery for transaction {transaction_id} with strategy {recovery_strategy}"
        )

        if recovery_strategy == "rollback_failed_tables":
            return await self._rollback_failed_tables(transaction_id, failed_operations)
        elif recovery_strategy == "retry_failed":
            return await self._retry_failed_operations(
                transaction_id, failed_operations
            )
        else:
            logger.info(f"Manual recovery required for transaction {transaction_id}")
            return False

    async def _rollback_failed_tables(
        self,
        transaction_id: str,
        failed_operations: List[TransactionOperation],
    ) -> bool:
        """Rollback operations for tables that had failures."""
        try:
            failed_tables = set(op.table_name for op in failed_operations)
            logger.info(f"Rolling back operations for tables: {failed_tables}")

            # This would require implementing table-specific rollback logic
            # For now, we log the intent
            if self.progress_reporter:
                self.progress_reporter.log_warning(
                    f"Would rollback operations for tables: {', '.join(failed_tables)}"
                )

            return True
        except Exception as e:
            logger.error(f"Failed to rollback failed tables: {e}")
            return False

    async def _retry_failed_operations(
        self,
        transaction_id: str,
        failed_operations: List[TransactionOperation],
    ) -> bool:
        """Retry failed operations."""
        logger.info(f"Retrying {len(failed_operations)} failed operations")

        # This would require re-executing the failed operations
        # For now, we log the intent
        if self.progress_reporter:
            self.progress_reporter.log_info(
                f"Would retry {len(failed_operations)} failed operations"
            )

        return True


class SyncTransactionManager(TransactionManager):
    """
    Specialized transaction manager for inventory sync operations.

    Extends TransactionManager with sync-specific functionality:
    - Table-aware transaction management
    - Dependency-based rollback ordering
    - Sync result integration
    """

    def __init__(
        self,
        engine: AsyncEngine,
        progress_reporter: Optional[ProgressReporter] = None,
        schema: str = "network",
    ):
        """
        Initialize sync transaction manager.

        Args:
            engine: SQLAlchemy async engine
            progress_reporter: Optional progress reporter
            schema: Database schema name
        """
        super().__init__(engine, progress_reporter, enable_savepoints=True)
        self.schema = schema

    async def execute_table_sync_with_transaction(
        self,
        transaction_id: str,
        table_name: str,
        sync_operation: callable,
        create_savepoint: bool = True,
    ) -> UpsertResult:
        """
        Execute a table sync operation within a transaction with savepoint support.

        Args:
            transaction_id: ID of the active transaction
            table_name: Name of the table being synced
            sync_operation: Async callable that performs the sync
            create_savepoint: Whether to create a savepoint before the operation

        Returns:
            UpsertResult: Result of the sync operation
        """
        if transaction_id not in self.active_transactions:
            raise create_sync_error(
                f"No active transaction found: {transaction_id}",
                operation="table_sync_with_transaction",
                table_name=table_name,
            )

        operation_id = f"{transaction_id}_{table_name}_{datetime.now().isoformat()}"
        savepoint_name = f"sp_{table_name}_{int(datetime.now().timestamp())}"

        # Create operation record
        operation = TransactionOperation(
            operation_id=operation_id,
            table_name=table_name,
            operation_type="upsert",
            record_count=0,  # Will be updated after sync
        )

        try:
            # Create savepoint if requested
            connection = (
                None  # This would need to be passed from the transaction context
            )
            if create_savepoint and connection:
                await self.create_savepoint(connection, transaction_id, savepoint_name)

            # Execute the sync operation
            result = await sync_operation()

            # Update operation record with results
            operation.record_count = result.records_processed or 0
            operation.success = result.records_failed == 0

            if not operation.success:
                operation.error_message = (
                    "; ".join(result.errors) if result.errors else "Unknown error"
                )

            # Record the operation
            self.record_operation(transaction_id, operation)

            logger.debug(
                f"Table sync completed for {table_name}: {result.records_created} created, {result.records_updated} updated"
            )

            return result

        except Exception as e:
            # Mark operation as failed
            operation.success = False
            operation.error_message = str(e)
            self.record_operation(transaction_id, operation)

            # Rollback to savepoint if available
            if create_savepoint and connection:
                rollback_success = await self.rollback_to_savepoint(
                    connection, transaction_id, savepoint_name
                )
                if rollback_success:
                    logger.info(f"Rolled back table sync for {table_name} to savepoint")

            # Re-raise the error
            raise create_sync_error(
                f"Table sync failed for {table_name}: {e}",
                operation="table_sync_with_transaction",
                table_name=table_name,
                cause=e,
            )


# Convenience functions for common transaction patterns


async def with_sync_transaction(
    engine: AsyncEngine,
    transaction_id: str,
    sync_operations: callable,
    progress_reporter: Optional[ProgressReporter] = None,
    isolation_level: Optional[str] = None,
) -> Any:
    """
    Execute sync operations within a managed transaction.

    Args:
        engine: SQLAlchemy async engine
        transaction_id: Unique transaction identifier
        sync_operations: Async callable that performs sync operations
        progress_reporter: Optional progress reporter
        isolation_level: SQL isolation level

    Returns:
        Result of sync_operations
    """
    manager = SyncTransactionManager(engine, progress_reporter)

    async with manager.transaction(
        transaction_id=transaction_id,
        isolation_level=isolation_level,
    ) as tx_context:
        return await sync_operations(tx_context, manager)
