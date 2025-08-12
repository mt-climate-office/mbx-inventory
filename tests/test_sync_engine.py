"""Tests for the SyncEngine class."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from mbx_inventory.cli.sync_engine import SyncEngine, SyncEngineError, TableSyncError
from mbx_inventory.cli.progress import ProgressReporter
from mbx_db.sync import UpsertResult, SyncResult


class MockInventory:
    """Mock inventory for testing."""

    def __init__(self):
        self.get_elements = MagicMock(
            return_value=[{"element_id": "E001", "name": "Temperature", "units": "C"}]
        )
        self.get_stations = MagicMock(
            return_value=[
                {
                    "station_id": "S001",
                    "name": "Test Station",
                    "latitude": 40.0,
                    "longitude": -105.0,
                }
            ]
        )
        self.get_component_models = MagicMock(
            return_value=[
                {
                    "component_model_id": "CM001",
                    "name": "Test Model",
                    "manufacturer": "Test Corp",
                }
            ]
        )
        self.get_inventory = MagicMock(
            return_value=[
                {
                    "inventory_id": "I001",
                    "component_model_id": "CM001",
                    "serial_number": "12345",
                }
            ]
        )
        self.get_deployments = MagicMock(
            return_value=[
                {"deployment_id": "D001", "station_id": "S001", "inventory_id": "I001"}
            ]
        )
        self.get_component_elements = MagicMock(
            return_value=[{"component_model_id": "CM001", "element_id": "E001"}]
        )
        self.get_request_schemas = MagicMock(
            return_value=[
                {"request_schema_id": "RS001", "element_id": "E001", "schema": "{}"}
            ]
        )
        self.get_response_schemas = MagicMock(
            return_value=[
                {"response_schema_id": "RSP001", "element_id": "E001", "schema": "{}"}
            ]
        )

    def reset_all_to_empty(self):
        """Reset all methods to return empty lists."""
        for method_name in [
            "get_elements",
            "get_stations",
            "get_component_models",
            "get_inventory",
            "get_deployments",
            "get_component_elements",
            "get_request_schemas",
            "get_response_schemas",
        ]:
            getattr(self, method_name).return_value = []


@pytest.fixture
def mock_inventory():
    """Create a mock inventory instance."""
    return MockInventory()


@pytest.fixture
def mock_db_engine():
    """Create a mock database engine."""
    return AsyncMock()


@pytest.fixture
def mock_progress_reporter():
    """Create a mock progress reporter."""
    reporter = MagicMock(spec=ProgressReporter)
    reporter.log_debug = MagicMock()
    reporter.start_operation = MagicMock()
    reporter.update_progress = MagicMock()
    reporter.complete_operation = MagicMock()
    reporter.report_error = MagicMock()
    return reporter


@pytest.fixture
def sync_engine(mock_inventory, mock_db_engine, mock_progress_reporter):
    """Create a SyncEngine instance for testing."""
    return SyncEngine(
        inventory=mock_inventory,
        db_engine=mock_db_engine,
        progress_reporter=mock_progress_reporter,
        schema="test_schema",
        batch_size=2,  # Small batch size for testing
    )


class TestSyncEngine:
    """Test cases for SyncEngine class."""

    def test_init(
        self, sync_engine, mock_inventory, mock_db_engine, mock_progress_reporter
    ):
        """Test SyncEngine initialization."""
        assert sync_engine.inventory == mock_inventory
        assert sync_engine.db_engine == mock_db_engine
        assert sync_engine.progress_reporter == mock_progress_reporter
        assert sync_engine.schema == "test_schema"
        assert sync_engine.batch_size == 2

    def test_get_available_tables(self, sync_engine):
        """Test getting available tables."""
        tables = sync_engine.get_available_tables()
        expected_tables = [
            "elements",
            "component_models",
            "stations",
            "inventory",
            "deployments",
            "component_elements",
            "request_schemas",
            "response_schemas",
        ]
        assert set(tables) == set(expected_tables)

    def test_get_table_dependencies(self, sync_engine):
        """Test getting table dependencies."""
        # Test table with no dependencies
        deps = sync_engine.get_table_dependencies("elements")
        assert deps == []

        # Test table with dependencies
        deps = sync_engine.get_table_dependencies("stations")
        assert deps == ["elements"]

        deps = sync_engine.get_table_dependencies("deployments")
        assert set(deps) == {"stations", "inventory"}

    def test_get_table_dependencies_unknown_table(self, sync_engine):
        """Test getting dependencies for unknown table."""
        with pytest.raises(SyncEngineError, match="Unknown table: unknown"):
            sync_engine.get_table_dependencies("unknown")

    def test_validate_table_filter(self, sync_engine):
        """Test table filter validation."""
        # Valid filter
        assert sync_engine.validate_table_filter(["elements", "stations"]) is True

        # Invalid filter
        assert sync_engine.validate_table_filter(["elements", "unknown"]) is False

        # Empty filter
        assert sync_engine.validate_table_filter([]) is True

    def test_get_tables_to_sync_no_filter(self, sync_engine):
        """Test getting tables to sync without filter."""
        tables = sync_engine._get_tables_to_sync(None)
        expected_tables = [
            "elements",
            "component_models",
            "stations",
            "inventory",
            "deployments",
            "component_elements",
            "request_schemas",
            "response_schemas",
        ]
        assert set(tables) == set(expected_tables)

    def test_get_tables_to_sync_with_filter(self, sync_engine):
        """Test getting tables to sync with filter."""
        tables = sync_engine._get_tables_to_sync(["elements", "stations"])
        assert tables == ["elements", "stations"]

    def test_get_tables_to_sync_unknown_table(self, sync_engine):
        """Test getting tables to sync with unknown table."""
        with pytest.raises(SyncEngineError, match="Unknown tables: \\['unknown'\\]"):
            sync_engine._get_tables_to_sync(["elements", "unknown"])

    def test_order_tables_by_dependencies(self, sync_engine):
        """Test ordering tables by dependencies."""
        tables = [
            "deployments",
            "stations",
            "elements",
            "inventory",
            "component_models",
        ]
        ordered = sync_engine._order_tables_by_dependencies(tables)

        # Elements should come before stations
        assert ordered.index("elements") < ordered.index("stations")

        # Stations should come before deployments
        assert ordered.index("stations") < ordered.index("deployments")

        # Component_models should come before inventory
        assert ordered.index("component_models") < ordered.index("inventory")

        # Inventory should come before deployments
        assert ordered.index("inventory") < ordered.index("deployments")

    def test_order_tables_by_dependencies_no_deps(self, sync_engine):
        """Test ordering tables with no dependencies."""
        tables = ["elements", "component_models"]
        ordered = sync_engine._order_tables_by_dependencies(tables)

        # Order should be preserved for tables with no dependencies
        assert set(ordered) == set(tables)

    @pytest.mark.asyncio
    async def test_sync_table_success(self, sync_engine, mock_inventory):
        """Test successful table sync."""
        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.elements",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_table("elements")

            assert result.table_name == "test_schema.elements"
            assert result.records_processed == 1
            assert result.records_created == 1
            assert result.records_updated == 0

            # Verify inventory method was called
            mock_inventory.get_elements.assert_called_once()

            # Verify sync_table_data was called with correct parameters
            mock_sync.assert_called_once()
            call_args = mock_sync.call_args
            assert call_args[1]["table_name"] == "elements"
            assert call_args[1]["schema"] == "test_schema"
            assert call_args[1]["dry_run"] is False
            assert call_args[1]["conflict_columns"] == ["element_id"]

    @pytest.mark.asyncio
    async def test_sync_table_dry_run(self, sync_engine, mock_inventory):
        """Test table sync in dry run mode."""
        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.elements",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_table("elements", dry_run=True)

            # Verify sync_table_data was called with dry_run=True
            call_args = mock_sync.call_args
            assert call_args[1]["dry_run"] is True

    @pytest.mark.asyncio
    async def test_sync_table_no_data(self, sync_engine, mock_inventory):
        """Test table sync with no data."""
        mock_inventory.get_elements.return_value = []

        result = await sync_engine.sync_table("elements")

        assert result.table_name == "test_schema.elements"
        assert result.records_processed == 0

    @pytest.mark.asyncio
    async def test_sync_table_unknown_table(self, sync_engine):
        """Test sync with unknown table."""
        with pytest.raises(TableSyncError, match="Unknown table: unknown"):
            await sync_engine.sync_table("unknown")

    @pytest.mark.asyncio
    async def test_sync_table_inventory_error(self, sync_engine, mock_inventory):
        """Test table sync when inventory method fails."""
        mock_inventory.get_elements.side_effect = Exception("Backend error")

        with pytest.raises(TableSyncError, match="Failed to sync table elements"):
            await sync_engine.sync_table("elements")

    @pytest.mark.asyncio
    async def test_sync_table_in_batches(self, sync_engine, mock_inventory):
        """Test syncing table data in batches."""
        # Create data larger than batch size (2)
        large_data = [
            {"element_id": f"E{i:03d}", "name": f"Element {i}", "units": "C"}
            for i in range(5)
        ]
        mock_inventory.get_elements.return_value = large_data

        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            # Mock successful batch results
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.elements",
                records_processed=2,
                records_created=2,
                records_updated=0,
            )

            result = await sync_engine.sync_table("elements")

            # Should have been called multiple times for batches
            assert mock_sync.call_count >= 2

            # Result should combine all batches
            assert result.records_processed == 5

    @pytest.mark.asyncio
    async def test_sync_all_tables_success(self, sync_engine):
        """Test successful sync of all tables."""
        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.test",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_all_tables()

            assert isinstance(result, SyncResult)
            assert result.total_tables > 0
            assert result.successful_tables > 0
            assert result.started_at is not None
            assert result.completed_at is not None

    @pytest.mark.asyncio
    async def test_sync_all_tables_with_filter(self, sync_engine):
        """Test sync with table filter."""
        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.elements",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_all_tables(table_filter=["elements"])

            assert result.total_tables == 1
            assert len(result.table_results) == 1
            assert result.table_results[0].table_name == "test_schema.elements"

    @pytest.mark.asyncio
    async def test_sync_all_tables_dry_run(self, sync_engine):
        """Test dry run sync of all tables."""
        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.test",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_all_tables(dry_run=True)

            # Verify all calls were made with dry_run=True
            for call in mock_sync.call_args_list:
                assert call[1]["dry_run"] is True

    @pytest.mark.asyncio
    async def test_sync_all_tables_partial_failure(self, sync_engine, mock_inventory):
        """Test sync with partial table failures."""
        # Make one table fail
        mock_inventory.get_stations.side_effect = Exception("Station sync failed")

        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.test",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            result = await sync_engine.sync_all_tables(
                table_filter=["elements", "stations"]
            )

            # Should have results for both tables (one success, one failure)
            assert result.total_tables == 2
            assert result.successful_tables == 1
            assert result.failed_tables == 1
            assert len(result.errors) > 0

    @pytest.mark.asyncio
    async def test_sync_all_tables_unknown_filter(self, sync_engine):
        """Test sync with unknown table in filter."""
        with pytest.raises(SyncEngineError, match="Unknown tables"):
            await sync_engine.sync_all_tables(table_filter=["unknown"])


class TestSyncEngineAdvancedFeatures:
    """Test advanced features of SyncEngine (task 5.2 requirements)."""

    @pytest.mark.asyncio
    async def test_selective_table_synchronization(self, sync_engine):
        """Test selective table synchronization (requirement 5.1)."""

        def mock_sync_side_effect(*args, **kwargs):
            table_name = kwargs.get("table_name", "unknown")
            return UpsertResult(
                table_name=f"test_schema.{table_name}",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

        with patch(
            "mbx_inventory.cli.sync_engine.sync_table_data",
            side_effect=mock_sync_side_effect,
        ):
            # Sync only specific tables
            result = await sync_engine.sync_all_tables(
                table_filter=["elements", "stations"]
            )

            # Should only sync the specified tables
            assert result.total_tables == 2
            synced_tables = [r.table_name.split(".")[-1] for r in result.table_results]
            assert set(synced_tables) == {"elements", "stations"}

    @pytest.mark.asyncio
    async def test_batch_processing_large_datasets(self, sync_engine, mock_inventory):
        """Test batch processing for large datasets (requirement 5.2)."""
        # Create a large dataset (larger than batch_size=2)
        large_dataset = [
            {"element_id": f"E{i:03d}", "name": f"Element {i}", "units": "C"}
            for i in range(10)
        ]
        mock_inventory.get_elements.return_value = large_dataset

        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.elements",
                records_processed=2,
                records_created=2,
                records_updated=0,
            )

            result = await sync_engine.sync_table("elements")

            # Should have been called multiple times for batches
            # 10 records / 2 batch_size = 5 batches
            assert mock_sync.call_count == 5

            # Each call should have batch_size records (except possibly the last)
            for call in mock_sync.call_args_list:
                data = call[1]["data"]
                assert len(data) <= sync_engine.batch_size

            # Total records processed should match original dataset
            assert result.records_processed == 10

    @pytest.mark.asyncio
    async def test_dependency_aware_table_ordering(self, sync_engine):
        """Test dependency-aware table ordering (requirement 5.3)."""
        # Test with tables that have complex dependencies
        tables_with_deps = [
            "deployments",
            "stations",
            "elements",
            "inventory",
            "component_models",
        ]

        def mock_sync_side_effect(*args, **kwargs):
            table_name = kwargs.get("table_name", "unknown")
            return UpsertResult(
                table_name=f"test_schema.{table_name}",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

        with patch(
            "mbx_inventory.cli.sync_engine.sync_table_data",
            side_effect=mock_sync_side_effect,
        ):
            result = await sync_engine.sync_all_tables(table_filter=tables_with_deps)

            # Extract the order in which tables were synced
            synced_order = [r.table_name.split(".")[-1] for r in result.table_results]

            # Should have attempted all tables
            assert len(synced_order) == 5

            # Verify dependency constraints are satisfied for tables that were synced
            if "elements" in synced_order and "stations" in synced_order:
                assert synced_order.index("elements") < synced_order.index("stations")

            if "component_models" in synced_order and "inventory" in synced_order:
                assert synced_order.index("component_models") < synced_order.index(
                    "inventory"
                )

            if "stations" in synced_order and "deployments" in synced_order:
                assert synced_order.index("stations") < synced_order.index(
                    "deployments"
                )

            if "inventory" in synced_order and "deployments" in synced_order:
                assert synced_order.index("inventory") < synced_order.index(
                    "deployments"
                )

    @pytest.mark.asyncio
    async def test_partial_failures_continue_processing(
        self, sync_engine, mock_inventory
    ):
        """Test handling partial failures and continuing processing (requirement 5.4)."""
        # Make one table fail while others succeed
        mock_inventory.get_stations.side_effect = Exception("Stations backend error")

        with patch("mbx_inventory.cli.sync_engine.sync_table_data") as mock_sync:
            mock_sync.return_value = UpsertResult(
                table_name="test_schema.test",
                records_processed=1,
                records_created=1,
                records_updated=0,
            )

            # Sync multiple tables including the failing one
            result = await sync_engine.sync_all_tables(
                table_filter=["elements", "stations", "component_models"]
            )

            # Should have attempted all tables
            assert result.total_tables == 3

            # Should have some successful and some failed
            assert result.successful_tables == 2  # elements and component_models
            assert result.failed_tables == 1  # stations

            # Should have error information
            assert len(result.errors) > 0
            assert any("stations" in error.lower() for error in result.errors)

            # Should have results for all tables (including failed ones)
            assert len(result.table_results) == 3

    @pytest.mark.asyncio
    async def test_batch_processing_with_partial_batch_failure(
        self, sync_engine, mock_inventory
    ):
        """Test batch processing handles partial batch failures gracefully."""
        # Create data that will be processed in batches
        large_dataset = [
            {"element_id": f"E{i:03d}", "name": f"Element {i}", "units": "C"}
            for i in range(5)
        ]
        mock_inventory.get_elements.return_value = large_dataset

        call_count = 0

        def mock_sync_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # Fail the second batch
                raise Exception("Batch 2 failed")
            return UpsertResult(
                table_name="test_schema.elements",
                records_processed=2,
                records_created=2,
                records_updated=0,
            )

        with patch(
            "mbx_inventory.cli.sync_engine.sync_table_data",
            side_effect=mock_sync_side_effect,
        ):
            result = await sync_engine.sync_table("elements")

            # Should have processed all records
            assert result.records_processed == 5

            # Should have some successful and some failed records
            assert result.records_created > 0  # From successful batches
            assert result.records_failed > 0  # From failed batch

            # Should have error information
            assert len(result.errors) > 0

    def test_table_filter_validation_integration(self, sync_engine):
        """Test integration of table filter validation."""
        # Valid filters should work
        assert sync_engine.validate_table_filter(["elements", "stations"]) is True

        # Invalid filters should be detected
        assert sync_engine.validate_table_filter(["elements", "nonexistent"]) is False

        # Empty filter should be valid (means all tables)
        assert sync_engine.validate_table_filter([]) is True

    def test_get_table_dependencies_complex(self, sync_engine):
        """Test getting dependencies for tables with complex dependency chains."""
        # Test table with multiple dependencies
        deps = sync_engine.get_table_dependencies("deployments")
        assert set(deps) == {"stations", "inventory"}

        # Test table with transitive dependencies
        deps = sync_engine.get_table_dependencies("component_elements")
        assert set(deps) == {"component_models", "elements"}

    def test_available_tables_completeness(self, sync_engine):
        """Test that all expected tables are available for sync."""
        available = sync_engine.get_available_tables()
        expected_tables = {
            "elements",
            "component_models",
            "stations",
            "inventory",
            "deployments",
            "component_elements",
            "request_schemas",
            "response_schemas",
        }
        assert set(available) == expected_tables


if __name__ == "__main__":
    pytest.main([__file__])
