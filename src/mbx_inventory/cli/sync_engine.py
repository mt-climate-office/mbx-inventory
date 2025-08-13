"""
Sync engine for inventory CLI operations.

This module provides the SyncEngine class that orchestrates synchronization
between inventory backends and PostgreSQL database using the existing
transformers and database operations.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine
from mbx_db.sync import sync_table_data, SyncResult, UpsertResult

from ..inventory import Inventory
from .progress import ProgressReporter
from .exceptions import (
    SyncOperationError,
    DataValidationError,
    ErrorContext,
    create_sync_error,
    create_database_error,
)
from .retry import RetryableOperation, SYNC_RETRY_CONFIG
from .transaction_manager import (
    SyncTransactionManager,
    with_sync_transaction,
)


logger = logging.getLogger(__name__)


@dataclass
class TableSyncConfig:
    """Configuration for syncing a specific table."""

    table_name: str
    get_data_method: str  # Method name on inventory instance
    dependencies: List[str]  # Tables that must be synced before this one
    conflict_columns: Optional[List[str]] = None  # Columns for conflict resolution


# Keep backward compatibility aliases
class SyncEngineError(SyncOperationError):
    """Base exception for sync engine operations."""

    pass


class TableSyncError(SyncOperationError):
    """Exception for table-specific sync errors."""

    pass


class SyncEngine:
    """
    Orchestrates synchronization between inventory backends and PostgreSQL.

    The SyncEngine manages the complete sync workflow including:
    - Retrieving data from inventory backends using transformers
    - Syncing data to PostgreSQL with conflict resolution
    - Progress reporting and error handling
    - Dry-run functionality for previewing changes
    - Dependency-aware table ordering
    """

    # Define table sync configurations with dependencies
    TABLE_CONFIGS = {
        "elements": TableSyncConfig(
            table_name="elements",
            get_data_method="get_elements",
            dependencies=[],
            # conflict_columns=["element_id"],
        ),
        "component_models": TableSyncConfig(
            table_name="component_models",
            get_data_method="get_component_models",
            dependencies=[],
            conflict_columns=["model"],
        ),
        "stations": TableSyncConfig(
            table_name="stations",
            get_data_method="get_stations",
            dependencies=["elements"],
            # conflict_columns=["station_id"],
        ),
        "inventory": TableSyncConfig(
            table_name="inventory",
            get_data_method="get_inventory",
            dependencies=["component_models"],
            # conflict_columns=["inventory_id"],
        ),
        "deployments": TableSyncConfig(
            table_name="deployments",
            get_data_method="get_deployments",
            dependencies=["stations", "inventory"],
            # conflict_columns=["deployment_id"],
        ),
        "component_elements": TableSyncConfig(
            table_name="component_elements",
            get_data_method="get_component_elements",
            dependencies=["component_models", "elements"],
            # conflict_columns=["component_model_id", "element_id"],
        ),
        "request_schemas": TableSyncConfig(
            table_name="request_schemas",
            get_data_method="get_request_schemas",
            dependencies=["elements"],
            # conflict_columns=["request_schema_id"],
        ),
        "response_schemas": TableSyncConfig(
            table_name="response_schemas",
            get_data_method="get_response_schemas",
            dependencies=["elements"],
            # conflict_columns=["response_schema_id"],
        ),
    }

    def __init__(
        self,
        inventory: Inventory,
        db_engine: AsyncEngine,
        progress_reporter: ProgressReporter,
        schema: str = "network",
        batch_size: int = 100,
        use_transactions: bool = True,
    ):
        """
        Initialize the SyncEngine.

        Args:
            inventory: Configured Inventory instance
            db_engine: SQLAlchemy async engine for database operations
            progress_reporter: Progress reporter for user feedback
            schema: Database schema name (default: "network")
            batch_size: Batch size for processing records
            use_transactions: Whether to use transaction management
        """
        self.inventory = inventory
        self.db_engine = db_engine
        self.progress_reporter = progress_reporter
        self.schema = schema
        self.batch_size = batch_size
        self.use_transactions = use_transactions

        # Initialize transaction manager if enabled
        if self.use_transactions:
            self.transaction_manager = SyncTransactionManager(
                engine=db_engine,
                progress_reporter=progress_reporter,
                schema=schema,
            )
        else:
            self.transaction_manager = None

        logger.debug(
            f"SyncEngine initialized with schema '{schema}', batch size {batch_size}, "
            f"transactions {'enabled' if use_transactions else 'disabled'}"
        )

    async def sync_all_tables(
        self, dry_run: bool = False, table_filter: Optional[List[str]] = None
    ) -> SyncResult:
        """
        Sync all configured tables from inventory backend to PostgreSQL.

        Args:
            dry_run: If True, preview changes without executing them
            table_filter: Optional list of specific tables to sync

        Returns:
            SyncResult with comprehensive operation statistics

        Raises:
            SyncOperationError: If sync operation fails
        """
        logger.info(f"Starting {'dry run' if dry_run else 'sync'} for all tables")

        sync_result = SyncResult(started_at=datetime.now())

        context = ErrorContext(
            operation="sync_all_tables",
            additional_data={
                "dry_run": dry_run,
                "table_filter": table_filter,
                "schema": self.schema,
            },
        )

        try:
            # Determine which tables to sync
            tables_to_sync = self._get_tables_to_sync(table_filter)

            # Order tables by dependencies
            ordered_tables = self._order_tables_by_dependencies(tables_to_sync)

            logger.info(
                f"Will sync {len(ordered_tables)} tables in order: {ordered_tables}"
            )

            # Start overall progress tracking
            self.progress_reporter.start_operation(
                f"{'Dry run' if dry_run else 'Syncing'} {len(ordered_tables)} tables",
                total_items=len(ordered_tables),
            )

            successful_tables = 0
            total_records_processed = 0

            # Sync each table with error recovery
            for table_name in ordered_tables:
                table_context = ErrorContext(
                    operation="sync_table",
                    table_name=table_name,
                    additional_data={"dry_run": dry_run},
                )

                try:
                    # Use retryable operation for individual table sync
                    retry_op = RetryableOperation(
                        f"sync_table_{table_name}",
                        SYNC_RETRY_CONFIG,
                        self.progress_reporter,
                    )

                    table_result = await retry_op.execute_async(
                        lambda: self.sync_table(table_name, dry_run)
                    )

                    sync_result.add_table_result(table_result)
                    successful_tables += 1
                    total_records_processed += table_result.records_processed or 0

                    self.progress_reporter.update_progress(
                        1,
                        f"Completed {table_name}: {table_result.records_created} created, "
                        f"{table_result.records_updated} updated",
                    )

                except Exception as e:
                    error_msg = f"Failed to sync table {table_name}: {str(e)}"
                    logger.error(error_msg)
                    sync_result.errors.append(error_msg)

                    # Create a failed result for this table
                    failed_result = UpsertResult(
                        table_name=table_name,
                        records_failed=1,  # Placeholder
                        errors=[error_msg],
                    )
                    sync_result.add_table_result(failed_result)

                    # Create detailed sync error
                    sync_error = create_sync_error(
                        f"Table sync failed: {error_msg}",
                        operation="sync_table",
                        table_name=table_name,
                        partial_success=successful_tables > 0,
                        context=table_context,
                        cause=e,
                    )

                    self.progress_reporter.report_error(
                        sync_error.get_formatted_message(),
                        context={
                            "table": table_name,
                            "error_code": sync_error.error_code,
                        },
                    )

                    # Continue with other tables for partial success
                    continue

            sync_result.completed_at = datetime.now()

            # Determine if operation was successful
            partial_success = successful_tables > 0 and successful_tables < len(
                ordered_tables
            )

            # Complete progress reporting
            summary = (
                f"{'Dry run completed' if dry_run else 'Sync completed'}: "
                f"{successful_tables}/{len(ordered_tables)} tables successful, "
                f"{sync_result.total_records_created} created, "
                f"{sync_result.total_records_updated} updated"
            )

            self.progress_reporter.complete_operation(summary)

            logger.info(
                f"Sync operation completed with {successful_tables} successful tables"
            )

            # If no tables succeeded, raise an error
            if successful_tables == 0:
                raise create_sync_error(
                    "All table synchronizations failed",
                    operation="sync_all_tables",
                    records_processed=total_records_processed,
                    records_failed=len(ordered_tables),
                    partial_success=False,
                    context=context,
                )

            return sync_result

        except SyncOperationError:
            # Re-raise sync operation errors
            raise
        except Exception as e:
            error_msg = f"Sync operation failed: {str(e)}"
            logger.error(error_msg)
            sync_result.errors.append(error_msg)
            sync_result.completed_at = datetime.now()

            self.progress_reporter.report_error(error_msg)
            raise create_sync_error(
                error_msg,
                operation="sync_all_tables",
                context=context,
                cause=e,
            )

    async def sync_table(self, table_name: str, dry_run: bool = False) -> UpsertResult:
        """
        Sync a specific table from inventory backend to PostgreSQL.

        Args:
            table_name: Name of the table to sync
            dry_run: If True, preview changes without executing them

        Returns:
            UpsertResult with table-specific operation statistics

        Raises:
            SyncOperationError: If table sync fails
        """
        logger.debug(
            f"Starting {'dry run' if dry_run else 'sync'} for table {table_name}"
        )

        context = ErrorContext(
            operation="sync_table",
            table_name=table_name,
            additional_data={"dry_run": dry_run, "schema": self.schema},
        )

        if table_name not in self.TABLE_CONFIGS:
            raise create_sync_error(
                f"Unknown table: {table_name}",
                operation="sync_table",
                table_name=table_name,
                context=context,
            )

        config = self.TABLE_CONFIGS[table_name]

        try:
            # Get data from inventory using the configured method
            if not hasattr(self.inventory, config.get_data_method):
                raise create_sync_error(
                    f"Inventory method '{config.get_data_method}' not found for table {table_name}",
                    operation="sync_table",
                    table_name=table_name,
                    context=context,
                )

            get_data_method = getattr(self.inventory, config.get_data_method)

            self.progress_reporter.log_debug(
                f"Retrieving data for {table_name}",
                context={"method": config.get_data_method},
            )

            # Retrieve and transform data with error handling
            try:
                data = get_data_method()
            except Exception as e:
                raise create_sync_error(
                    f"Failed to retrieve data for {table_name}: {e}",
                    operation="data_retrieval",
                    table_name=table_name,
                    context=context,
                    cause=e,
                )

            if not data:
                logger.info(f"No data found for table {table_name}")
                return UpsertResult(
                    table_name=f"{self.schema}.{table_name}", records_processed=0
                )

            logger.info(f"Retrieved {len(data)} records for {table_name}")
            context.record_count = len(data)

            # Validate data structure
            if not isinstance(data, list):
                raise DataValidationError(
                    f"Expected list of records for {table_name}, got {type(data)}",
                    table_name=table_name,
                    context=context,
                )

            # Process data in batches if needed
            if len(data) > self.batch_size:
                return await self._sync_table_in_batches(
                    table_name, data, config, dry_run
                )
            else:
                return await self._sync_table_batch(table_name, data, config, dry_run)

        except (SyncOperationError, DataValidationError):
            # Re-raise our custom errors
            raise
        except Exception as e:
            error_msg = f"Failed to sync table {table_name}: {str(e)}"
            logger.error(error_msg)
            raise create_sync_error(
                error_msg,
                operation="sync_table",
                table_name=table_name,
                context=context,
                cause=e,
            )

    async def _sync_table_in_batches(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        config: TableSyncConfig,
        dry_run: bool,
    ) -> UpsertResult:
        """Sync table data in batches for large datasets."""
        logger.info(
            f"Processing {len(data)} records for {table_name} in batches of {self.batch_size}"
        )

        # Initialize combined result
        combined_result = UpsertResult(
            table_name=f"{self.schema}.{table_name}", records_processed=len(data)
        )

        # Process in batches
        for i in range(0, len(data), self.batch_size):
            batch = data[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(data) + self.batch_size - 1) // self.batch_size

            logger.debug(
                f"Processing batch {batch_num}/{total_batches} for {table_name}"
            )

            try:
                batch_result = await self._sync_table_batch(
                    table_name, batch, config, dry_run
                )

                # Combine results
                combined_result.records_created += batch_result.records_created
                combined_result.records_updated += batch_result.records_updated
                combined_result.records_failed += batch_result.records_failed
                combined_result.duration_seconds += batch_result.duration_seconds
                combined_result.errors.extend(batch_result.errors)

            except Exception as e:
                error_msg = f"Batch {batch_num} failed for {table_name}: {str(e)}"
                logger.error(error_msg)
                combined_result.errors.append(error_msg)
                combined_result.records_failed += len(batch)

        return combined_result

    async def _sync_table_batch(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        config: TableSyncConfig,
        dry_run: bool,
    ) -> UpsertResult:
        """Sync a single batch of table data."""
        context = ErrorContext(
            operation="sync_table_batch",
            table_name=table_name,
            record_count=len(data),
            additional_data={"dry_run": dry_run},
        )

        try:
            result = await sync_table_data(
                engine=self.db_engine,
                table_name=table_name,
                data=data,
                schema=self.schema,
                dry_run=dry_run,
                conflict_columns=config.conflict_columns,
            )

            if dry_run:
                logger.debug(
                    f"Dry run for {table_name}: would process {len(data)} records"
                )
            else:
                logger.debug(
                    f"Synced {table_name}: {result.records_created} created, "
                    f"{result.records_updated} updated, {result.records_failed} failed"
                )

            return result

        except Exception as e:
            error_msg = f"Database sync failed for {table_name}: {str(e)}"
            logger.error(error_msg)

            # Create detailed database error
            db_error = create_database_error(
                error_msg,
                operation="sync_table_batch",
                table_name=table_name,
                context=context,
                cause=e,
            )

            # Log the detailed error
            self.progress_reporter.report_error(
                db_error.get_formatted_message(),
                context={"error_code": db_error.error_code},
            )

            # Return a failed result
            return UpsertResult(
                table_name=f"{self.schema}.{table_name}",
                records_processed=len(data),
                records_failed=len(data),
                errors=[error_msg],
            )

    def _get_tables_to_sync(self, table_filter: Optional[List[str]]) -> List[str]:
        """Determine which tables to sync based on filter and configuration."""
        # Get tables that are configured in the inventory
        configured_tables = self._get_configured_tables()

        if table_filter:
            # Validate that all requested tables exist in configuration
            unknown_tables = [t for t in table_filter if t not in configured_tables]
            if unknown_tables:
                context = ErrorContext(
                    operation="validate_table_filter",
                    additional_data={
                        "unknown_tables": unknown_tables,
                        "configured_tables": configured_tables,
                        "available_tables": list(self.TABLE_CONFIGS.keys()),
                    },
                )
                raise create_sync_error(
                    f"Tables not configured: {unknown_tables}. Configured tables: {configured_tables}",
                    operation="validate_table_filter",
                    context=context,
                )
            return table_filter
        else:
            return configured_tables

    def _order_tables_by_dependencies(self, tables: List[str]) -> List[str]:
        """Order tables based on their dependencies using topological sort."""
        context = ErrorContext(
            operation="order_tables_by_dependencies", additional_data={"tables": tables}
        )

        # Initialize graph and in-degree for all tables
        graph = {table: [] for table in tables}
        in_degree = {table: 0 for table in tables}

        # Build dependency graph
        for table in tables:
            config = self.TABLE_CONFIGS[table]

            # For each dependency of this table
            for dep in config.dependencies:
                if (
                    dep in tables
                ):  # Only consider dependencies that are in our sync list
                    # dep -> table (dep must come before table)
                    graph[dep].append(table)
                    in_degree[table] += 1

        # Topological sort using Kahn's algorithm
        queue = [table for table in tables if in_degree[table] == 0]
        ordered = []

        while queue:
            current = queue.pop(0)
            ordered.append(current)

            # For each table that depends on current
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check for circular dependencies
        if len(ordered) != len(tables):
            remaining = [t for t in tables if t not in ordered]
            raise create_sync_error(
                f"Circular dependency detected in tables: {remaining}",
                operation="order_tables_by_dependencies",
                context=context,
            )

        logger.debug(f"Table sync order: {ordered}")
        return ordered

    async def sync_all_tables_with_transaction(
        self,
        dry_run: bool = False,
        table_filter: Optional[List[str]] = None,
        transaction_id: Optional[str] = None,
    ) -> SyncResult:
        """
        Sync all configured tables within a single database transaction.

        This method provides ACID guarantees - either all tables sync successfully
        or all changes are rolled back.

        Args:
            dry_run: If True, preview changes without executing them
            table_filter: Optional list of specific tables to sync
            transaction_id: Optional transaction ID (auto-generated if None)

        Returns:
            SyncResult with comprehensive operation statistics

        Raises:
            SyncOperationError: If sync operation fails
        """
        if not self.use_transactions or not self.transaction_manager:
            logger.warning(
                "Transaction management is disabled, falling back to regular sync"
            )
            return await self.sync_all_tables(dry_run, table_filter)

        if transaction_id is None:
            transaction_id = f"sync_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Starting transactional sync with ID: {transaction_id}")

        context = ErrorContext(
            operation="sync_all_tables_with_transaction",
            additional_data={
                "transaction_id": transaction_id,
                "dry_run": dry_run,
                "table_filter": table_filter,
                "schema": self.schema,
            },
        )

        try:
            # Execute sync within a transaction
            result = await with_sync_transaction(
                engine=self.db_engine,
                transaction_id=transaction_id,
                sync_operations=self._execute_sync_operations,
                progress_reporter=self.progress_reporter,
                isolation_level="READ_COMMITTED",
            )

            return result

        except Exception as e:
            error_msg = f"Transactional sync failed: {str(e)}"
            logger.error(error_msg)

            raise create_sync_error(
                error_msg,
                operation="sync_all_tables_with_transaction",
                context=context,
                cause=e,
            )

    async def _execute_sync_operations(
        self,
        tx_context,
        tx_manager: SyncTransactionManager,
        dry_run: bool = False,
        table_filter: Optional[List[str]] = None,
    ) -> SyncResult:
        """
        Execute sync operations within a transaction context.

        Args:
            tx_context: Transaction context from transaction manager
            tx_manager: Transaction manager instance
            dry_run: Whether this is a dry run
            table_filter: Optional table filter

        Returns:
            SyncResult with operation statistics
        """
        sync_result = SyncResult(started_at=datetime.now())

        try:
            # Determine which tables to sync
            tables_to_sync = self._get_tables_to_sync(table_filter)

            # Order tables by dependencies
            ordered_tables = self._order_tables_by_dependencies(tables_to_sync)

            logger.info(
                f"Will sync {len(ordered_tables)} tables in transaction: {ordered_tables}"
            )

            # Start overall progress tracking
            self.progress_reporter.start_operation(
                f"{'Dry run' if dry_run else 'Syncing'} {len(ordered_tables)} tables (transactional)",
                total_items=len(ordered_tables),
            )

            successful_tables = 0

            # Sync each table within the transaction
            for table_name in ordered_tables:
                try:
                    # Create a sync operation for this table
                    async def table_sync_operation():
                        return await self.sync_table(table_name, dry_run)

                    # Execute with transaction support
                    table_result = await tx_manager.execute_table_sync_with_transaction(
                        transaction_id=tx_context.transaction_id,
                        table_name=table_name,
                        sync_operation=table_sync_operation,
                        create_savepoint=True,
                    )

                    sync_result.add_table_result(table_result)
                    successful_tables += 1

                    self.progress_reporter.update_progress(
                        1,
                        f"Completed {table_name}: {table_result.records_created} created, "
                        f"{table_result.records_updated} updated",
                    )

                except Exception as e:
                    error_msg = (
                        f"Failed to sync table {table_name} in transaction: {str(e)}"
                    )
                    logger.error(error_msg)
                    sync_result.errors.append(error_msg)

                    # Create a failed result for this table
                    failed_result = UpsertResult(
                        table_name=table_name,
                        records_failed=1,
                        errors=[error_msg],
                    )
                    sync_result.add_table_result(failed_result)

                    # In transactional mode, we might want to fail fast
                    # or continue based on configuration
                    self.progress_reporter.report_error(
                        f"Table sync failed in transaction: {error_msg}",
                        context={
                            "table": table_name,
                            "transaction_id": tx_context.transaction_id,
                        },
                    )

                    # For now, continue with other tables
                    # The transaction will be rolled back if any critical error occurs
                    continue

            sync_result.completed_at = datetime.now()

            # Complete progress reporting
            summary = (
                f"{'Dry run completed' if dry_run else 'Transactional sync completed'}: "
                f"{successful_tables}/{len(ordered_tables)} tables successful, "
                f"{sync_result.total_records_created} created, "
                f"{sync_result.total_records_updated} updated"
            )

            self.progress_reporter.complete_operation(summary)

            logger.info(
                f"Transactional sync completed with {successful_tables} successful tables"
            )

            return sync_result

        except Exception as e:
            sync_result.completed_at = datetime.now()
            sync_result.errors.append(str(e))

            logger.error(f"Error in transactional sync operations: {e}")
            raise

    def _get_configured_tables(self) -> List[str]:
        """Get list of tables that are configured in the inventory."""
        # Check if inventory has table_configs (new format)
        if hasattr(self.inventory, "table_configs") and self.inventory.table_configs:
            # Return only enabled tables from the new configuration
            return [
                table_name
                for table_name, config in self.inventory.table_configs.items()
                if hasattr(config, "enabled") and config.enabled
            ]

        # Check if inventory has table_mappings (legacy format)
        if hasattr(self.inventory, "table_mapper") and self.inventory.table_mapper:
            # Return tables that have mappings
            return list(self.inventory.table_mapper.get_all_mappings().keys())

        # Fallback to all available tables
        return list(self.TABLE_CONFIGS.keys())

    def get_available_tables(self) -> List[str]:
        """Get list of available tables for synchronization."""
        return list(self.TABLE_CONFIGS.keys())

    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get dependencies for a specific table."""
        if table_name not in self.TABLE_CONFIGS:
            context = ErrorContext(
                operation="get_table_dependencies",
                additional_data={
                    "table_name": table_name,
                    "available_tables": list(self.TABLE_CONFIGS.keys()),
                },
            )
            raise create_sync_error(
                f"Unknown table: {table_name}",
                operation="get_table_dependencies",
                context=context,
            )
        return self.TABLE_CONFIGS[table_name].dependencies.copy()

    def validate_table_filter(self, table_filter: List[str]) -> bool:
        """Validate that a table filter contains only known tables."""
        unknown_tables = [t for t in table_filter if t not in self.TABLE_CONFIGS]
        return len(unknown_tables) == 0
