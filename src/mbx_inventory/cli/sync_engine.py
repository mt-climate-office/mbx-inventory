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


logger = logging.getLogger(__name__)


@dataclass
class TableSyncConfig:
    """Configuration for syncing a specific table."""

    table_name: str
    get_data_method: str  # Method name on inventory instance
    dependencies: List[str]  # Tables that must be synced before this one
    conflict_columns: Optional[List[str]] = None  # Columns for conflict resolution


class SyncEngineError(Exception):
    """Base exception for sync engine operations."""

    pass


class TableSyncError(SyncEngineError):
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
            conflict_columns=["element_id"],
        ),
        "component_models": TableSyncConfig(
            table_name="component_models",
            get_data_method="get_component_models",
            dependencies=[],
            conflict_columns=["component_model_id"],
        ),
        "stations": TableSyncConfig(
            table_name="stations",
            get_data_method="get_stations",
            dependencies=["elements"],
            conflict_columns=["station_id"],
        ),
        "inventory": TableSyncConfig(
            table_name="inventory",
            get_data_method="get_inventory",
            dependencies=["component_models"],
            conflict_columns=["inventory_id"],
        ),
        "deployments": TableSyncConfig(
            table_name="deployments",
            get_data_method="get_deployments",
            dependencies=["stations", "inventory"],
            conflict_columns=["deployment_id"],
        ),
        "component_elements": TableSyncConfig(
            table_name="component_elements",
            get_data_method="get_component_elements",
            dependencies=["component_models", "elements"],
            conflict_columns=["component_model_id", "element_id"],
        ),
        "request_schemas": TableSyncConfig(
            table_name="request_schemas",
            get_data_method="get_request_schemas",
            dependencies=["elements"],
            conflict_columns=["request_schema_id"],
        ),
        "response_schemas": TableSyncConfig(
            table_name="response_schemas",
            get_data_method="get_response_schemas",
            dependencies=["elements"],
            conflict_columns=["response_schema_id"],
        ),
    }

    def __init__(
        self,
        inventory: Inventory,
        db_engine: AsyncEngine,
        progress_reporter: ProgressReporter,
        schema: str = "network",
        batch_size: int = 100,
    ):
        """
        Initialize the SyncEngine.

        Args:
            inventory: Configured Inventory instance
            db_engine: SQLAlchemy async engine for database operations
            progress_reporter: Progress reporter for user feedback
            schema: Database schema name (default: "network")
            batch_size: Batch size for processing records
        """
        self.inventory = inventory
        self.db_engine = db_engine
        self.progress_reporter = progress_reporter
        self.schema = schema
        self.batch_size = batch_size

        logger.debug(
            f"SyncEngine initialized with schema '{schema}' and batch size {batch_size}"
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
            SyncEngineError: If sync operation fails
        """
        logger.info(f"Starting {'dry run' if dry_run else 'sync'} for all tables")

        sync_result = SyncResult(started_at=datetime.now())

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

            # Sync each table
            for table_name in ordered_tables:
                try:
                    table_result = await self.sync_table(table_name, dry_run)
                    sync_result.add_table_result(table_result)

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

                    self.progress_reporter.report_error(
                        error_msg, context={"table": table_name}
                    )

                    # Continue with other tables
                    continue

            sync_result.completed_at = datetime.now()

            # Complete progress reporting
            summary = (
                f"{'Dry run completed' if dry_run else 'Sync completed'}: "
                f"{sync_result.successful_tables}/{sync_result.total_tables} tables successful, "
                f"{sync_result.total_records_created} created, "
                f"{sync_result.total_records_updated} updated"
            )

            self.progress_reporter.complete_operation(summary)

            logger.info(
                f"Sync operation completed with {sync_result.successful_tables} successful tables"
            )
            return sync_result

        except Exception as e:
            error_msg = f"Sync operation failed: {str(e)}"
            logger.error(error_msg)
            sync_result.errors.append(error_msg)
            sync_result.completed_at = datetime.now()

            self.progress_reporter.report_error(error_msg)
            raise SyncEngineError(error_msg) from e

    async def sync_table(self, table_name: str, dry_run: bool = False) -> UpsertResult:
        """
        Sync a specific table from inventory backend to PostgreSQL.

        Args:
            table_name: Name of the table to sync
            dry_run: If True, preview changes without executing them

        Returns:
            UpsertResult with table-specific operation statistics

        Raises:
            TableSyncError: If table sync fails
        """
        logger.debug(
            f"Starting {'dry run' if dry_run else 'sync'} for table {table_name}"
        )

        if table_name not in self.TABLE_CONFIGS:
            raise TableSyncError(f"Unknown table: {table_name}")

        config = self.TABLE_CONFIGS[table_name]

        try:
            # Get data from inventory using the configured method
            get_data_method = getattr(self.inventory, config.get_data_method)

            self.progress_reporter.log_debug(
                f"Retrieving data for {table_name}",
                context={"method": config.get_data_method},
            )

            # Retrieve and transform data
            data = get_data_method()

            if not data:
                logger.info(f"No data found for table {table_name}")
                return UpsertResult(
                    table_name=f"{self.schema}.{table_name}", records_processed=0
                )

            logger.info(f"Retrieved {len(data)} records for {table_name}")

            # Process data in batches if needed
            if len(data) > self.batch_size:
                return await self._sync_table_in_batches(
                    table_name, data, config, dry_run
                )
            else:
                return await self._sync_table_batch(table_name, data, config, dry_run)

        except Exception as e:
            error_msg = f"Failed to sync table {table_name}: {str(e)}"
            logger.error(error_msg)
            raise TableSyncError(error_msg) from e

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

            # Return a failed result
            return UpsertResult(
                table_name=f"{self.schema}.{table_name}",
                records_processed=len(data),
                records_failed=len(data),
                errors=[error_msg],
            )

    def _get_tables_to_sync(self, table_filter: Optional[List[str]]) -> List[str]:
        """Determine which tables to sync based on filter."""
        if table_filter:
            # Validate that all requested tables exist
            unknown_tables = [t for t in table_filter if t not in self.TABLE_CONFIGS]
            if unknown_tables:
                raise SyncEngineError(f"Unknown tables: {unknown_tables}")
            return table_filter
        else:
            return list(self.TABLE_CONFIGS.keys())

    def _order_tables_by_dependencies(self, tables: List[str]) -> List[str]:
        """Order tables based on their dependencies using topological sort."""
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
            raise SyncEngineError(
                f"Circular dependency detected in tables: {remaining}"
            )

        logger.debug(f"Table sync order: {ordered}")
        return ordered

    def get_available_tables(self) -> List[str]:
        """Get list of available tables for synchronization."""
        return list(self.TABLE_CONFIGS.keys())

    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get dependencies for a specific table."""
        if table_name not in self.TABLE_CONFIGS:
            raise SyncEngineError(f"Unknown table: {table_name}")
        return self.TABLE_CONFIGS[table_name].dependencies.copy()

    def validate_table_filter(self, table_filter: List[str]) -> bool:
        """Validate that a table filter contains only known tables."""
        unknown_tables = [t for t in table_filter if t not in self.TABLE_CONFIGS]
        return len(unknown_tables) == 0
