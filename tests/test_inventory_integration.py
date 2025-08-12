"""
Integration tests for Inventory class with NetworkSchemaMixin.
"""

import pytest
from unittest.mock import Mock
from mbx_inventory.inventory import Inventory
from mbx_inventory.transformers import BackendError


class MockAirTableBackend:
    """Mock AirTable backend for testing."""

    def __init__(self):
        self.read_records = Mock()
        self.create_records = Mock()
        self.update_records = Mock()
        self.validate = Mock(return_value=True)


class TestInventoryIntegration:
    """Integration tests for Inventory class with both generic and network schema methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_backend = MockAirTableBackend()

    def test_inventory_initialization_default(self):
        """Test Inventory initialization with default settings."""
        inventory = Inventory(self.mock_backend)

        # Should have both generic and network schema capabilities
        assert hasattr(inventory, "read")  # Generic method
        assert hasattr(inventory, "create")  # Generic method
        assert hasattr(inventory, "update")  # Generic method
        assert hasattr(inventory, "get_elements")  # Network schema method
        assert hasattr(inventory, "get_stations")  # Network schema method

        # Should use default AirTable backend type
        assert inventory.backend_type == "airtable"
        assert inventory.table_mapper.get_backend_table_name("elements") == "Elements"

    def test_inventory_initialization_custom_backend_type(self):
        """Test Inventory initialization with custom backend type."""
        inventory = Inventory(self.mock_backend, backend_type="baserow")

        assert inventory.backend_type == "baserow"
        assert inventory.table_mapper.get_backend_table_name("elements") == "Elements"

    def test_inventory_initialization_custom_table_mappings(self):
        """Test Inventory initialization with custom table mappings."""
        custom_mappings = {
            "elements": "Sensor Elements",
            "stations": "Weather Stations",
        }

        inventory = Inventory(
            self.mock_backend, table_mappings=custom_mappings, backend_type="airtable"
        )

        assert (
            inventory.table_mapper.get_backend_table_name("elements")
            == "Sensor Elements"
        )
        assert (
            inventory.table_mapper.get_backend_table_name("stations")
            == "Weather Stations"
        )

    def test_generic_crud_methods_work(self):
        """Test that generic CRUD methods still work as before."""
        inventory = Inventory(self.mock_backend)

        # Test read
        mock_data = [{"id": "rec1", "field": "value"}]
        self.mock_backend.read_records.return_value = mock_data

        result = inventory.read("custom_table", filter="value")

        self.mock_backend.read_records.assert_called_once_with(
            "custom_table", filter="value"
        )
        assert result == mock_data

        # Test create
        records_to_create = [{"field": "new_value"}]
        created_records = [{"id": "rec2", "field": "new_value"}]
        self.mock_backend.create_records.return_value = created_records

        result = inventory.create("custom_table", records_to_create, batch_size=10)

        self.mock_backend.create_records.assert_called_once_with(
            "custom_table", records_to_create, batch_size=10
        )
        assert result == created_records

        # Test update
        records_to_update = [{"id": "rec1", "field": "updated_value"}]
        updated_records = [{"id": "rec1", "field": "updated_value"}]
        self.mock_backend.update_records.return_value = updated_records

        result = inventory.update("custom_table", records_to_update)

        self.mock_backend.update_records.assert_called_once_with(
            "custom_table", records_to_update
        )
        assert result == updated_records

    def test_network_schema_methods_work(self):
        """Test that network schema methods work correctly."""
        inventory = Inventory(self.mock_backend)

        # Test get_elements
        mock_elements_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": "Celsius",
                "US Units": "Fahrenheit",
            }
        ]
        self.mock_backend.read_records.return_value = mock_elements_data

        result = inventory.get_elements()

        # Should call backend with AirTable table name
        self.mock_backend.read_records.assert_called_once_with("Elements")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["element"] == "TEMP"
        assert result[0]["description"] == "Air Temperature"
        assert result[0]["description_short"] == "Temp"
        assert result[0]["si_units"] == "Celsius"
        assert result[0]["us_units"] == "Fahrenheit"

    def test_network_schema_methods_with_filters(self):
        """Test network schema methods with filters."""
        inventory = Inventory(self.mock_backend)

        # Mock multiple stations
        mock_stations_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare",
                "Status": "active",
                "Latitude": "41.9742",
                "Longitude": "-87.9073",
                "Elevation": "201.5",
            },
            {
                "id": "rec2",
                "Station": "KMDW",
                "Name": "Chicago Midway",
                "Status": "inactive",
                "Latitude": "41.7868",
                "Longitude": "-87.7522",
                "Elevation": "188.4",
            },
        ]
        self.mock_backend.read_records.return_value = mock_stations_data

        # Filter for active stations only
        result = inventory.get_stations(filters={"status": "active"})

        # Should return only active stations
        assert len(result) == 1
        assert result[0]["station"] == "KORD"
        assert result[0]["status"] == "active"

    def test_backend_validation_works(self):
        """Test that backend validation works."""
        inventory = Inventory(self.mock_backend)

        # Test successful validation
        self.mock_backend.validate.return_value = True
        assert inventory.validate() is True

        # Test failed validation
        self.mock_backend.validate.return_value = False
        assert inventory.validate() is False

    def test_both_methods_use_same_backend(self):
        """Test that both generic and network schema methods use the same backend."""
        inventory = Inventory(self.mock_backend)

        # Mock backend responses
        self.mock_backend.read_records.return_value = []

        # Call generic method
        inventory.read("custom_table")

        # Call network schema method
        inventory.get_elements()

        # Both should have called the same backend
        assert self.mock_backend.read_records.call_count == 2
        calls = self.mock_backend.read_records.call_args_list
        assert calls[0][0] == ("custom_table",)  # Generic call
        assert calls[1][0] == ("Elements",)  # Network schema call

    def test_error_handling_consistency(self):
        """Test that error handling is consistent between generic and network schema methods."""
        inventory = Inventory(self.mock_backend)

        # Mock backend to raise an exception
        self.mock_backend.read_records.side_effect = Exception(
            "Backend connection failed"
        )

        # Generic method should raise the raw exception
        with pytest.raises(Exception) as exc_info:
            inventory.read("custom_table")
        assert "Backend connection failed" in str(exc_info.value)

        # Network schema method should wrap it in BackendError
        with pytest.raises(BackendError) as exc_info:
            inventory.get_elements()
        assert "Failed to retrieve data from backend" in str(exc_info.value)
        assert "Backend connection failed" in str(exc_info.value)

    def test_backend_type_affects_table_mappings(self):
        """Test that backend type affects table name mappings for network schema methods."""
        # Test AirTable backend
        airtable_inventory = Inventory(self.mock_backend, backend_type="airtable")
        self.mock_backend.read_records.return_value = []

        airtable_inventory.get_elements()
        self.mock_backend.read_records.assert_called_with("Elements")  # AirTable style

        # Test Baserow backend
        self.mock_backend.read_records.reset_mock()
        baserow_inventory = Inventory(self.mock_backend, backend_type="baserow")

        baserow_inventory.get_elements()
        self.mock_backend.read_records.assert_called_with("Elements")  # Baserow style

    def test_all_network_schema_methods_available(self):
        """Test that all network schema methods are available and work."""
        inventory = Inventory(self.mock_backend)

        # Mock backend to return empty data for all calls
        self.mock_backend.read_records.return_value = []

        # Test all network schema methods
        methods_and_tables = [
            (inventory.get_elements, "Elements"),
            (inventory.get_component_models, "Component Models"),
            (inventory.get_stations, "Stations"),
            (inventory.get_inventory, "Inventory"),
            (inventory.get_deployments, "Deployments"),
            (inventory.get_component_elements, "Component Elements"),
            (inventory.get_request_schemas, "Request Schemas"),
            (inventory.get_response_schemas, "Response Schemas"),
        ]

        for method, expected_table in methods_and_tables:
            self.mock_backend.read_records.reset_mock()
            result = method()

            # Should call backend with correct table name
            self.mock_backend.read_records.assert_called_once_with(expected_table)

            # Should return empty list for empty data
            assert result == []

    def test_backward_compatibility(self):
        """Test that existing code using generic methods still works."""
        # This simulates existing code that only uses generic methods
        inventory = Inventory(self.mock_backend)

        # Mock some generic operations
        self.mock_backend.read_records.return_value = [{"id": "rec1", "data": "value"}]
        self.mock_backend.create_records.return_value = [
            {"id": "rec2", "data": "new_value"}
        ]
        self.mock_backend.update_records.return_value = [
            {"id": "rec1", "data": "updated_value"}
        ]

        # These should work exactly as before
        read_result = inventory.read("any_table")
        create_result = inventory.create("any_table", [{"data": "new_value"}])
        update_result = inventory.update(
            "any_table", [{"id": "rec1", "data": "updated_value"}]
        )

        # Results should be unchanged
        assert read_result == [{"id": "rec1", "data": "value"}]
        assert create_result == [{"id": "rec2", "data": "new_value"}]
        assert update_result == [{"id": "rec1", "data": "updated_value"}]

        # Backend should have been called with the exact arguments
        self.mock_backend.read_records.assert_called_with("any_table")
        self.mock_backend.create_records.assert_called_with(
            "any_table", [{"data": "new_value"}]
        )
        self.mock_backend.update_records.assert_called_with(
            "any_table", [{"id": "rec1", "data": "updated_value"}]
        )
