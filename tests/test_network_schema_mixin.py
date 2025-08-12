"""
Unit tests for NetworkSchemaMixin.
"""

import pytest
from unittest.mock import Mock
from mbx_inventory.network_schema_mixin import NetworkSchemaMixin
from mbx_inventory.transformers import BackendError, TransformationError


class MockInventoryBackend:
    """Mock inventory backend for testing."""

    def __init__(self):
        self.read_records = Mock()
        self.create_records = Mock()
        self.update_records = Mock()
        self.validate = Mock(return_value=True)


class TestNetworkSchemaMixin:
    """Test cases for NetworkSchemaMixin base functionality."""

    def setup_method(self):
        """Set up test fixtures."""

        # Create a test class that inherits from NetworkSchemaMixin
        class TestInventory(NetworkSchemaMixin):
            def __init__(self, backend, **kwargs):
                self.backend = backend
                super().__init__(**kwargs)

        self.TestInventory = TestInventory
        self.mock_backend = MockInventoryBackend()

    def test_init_default_airtable(self):
        """Test initialization with default AirTable backend."""
        inventory = self.TestInventory(self.mock_backend)

        assert inventory.backend_type == "airtable"
        assert inventory.table_mapper is not None

        # Test default AirTable mapping
        assert inventory.table_mapper.get_backend_table_name("elements") == "Elements"
        assert (
            inventory.table_mapper.get_backend_table_name("component_models")
            == "Component Models"
        )

    def test_init_custom_backend_type(self):
        """Test initialization with custom backend type."""
        inventory = self.TestInventory(self.mock_backend, backend_type="baserow")

        assert inventory.backend_type == "baserow"

        # Test Baserow mapping
        assert inventory.table_mapper.get_backend_table_name("elements") == "Elements"
        assert (
            inventory.table_mapper.get_backend_table_name("component_models")
            == "Component Models"
        )

    def test_init_custom_table_mappings(self):
        """Test initialization with custom table mappings."""
        custom_mappings = {
            "elements": "Sensor Elements",
            "stations": "Weather Stations",
        }

        inventory = self.TestInventory(
            self.mock_backend, table_mappings=custom_mappings, backend_type="airtable"
        )

        # Custom mappings should override defaults
        assert (
            inventory.table_mapper.get_backend_table_name("elements")
            == "Sensor Elements"
        )
        assert (
            inventory.table_mapper.get_backend_table_name("stations")
            == "Weather Stations"
        )

        # Non-overridden mappings should use defaults
        assert inventory.table_mapper.get_backend_table_name("inventory") == "Inventory"

    def test_get_backend_data_success(self):
        """Test successful backend data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_data = [
            {"id": "rec1", "Element": "TEMP", "Description": "Temperature"},
            {"id": "rec2", "Element": "HUMID", "Description": "Humidity"},
        ]
        self.mock_backend.read_records.return_value = mock_data

        result = inventory._get_backend_data("elements")

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Elements")
        assert result == mock_data

    def test_get_backend_data_with_kwargs(self):
        """Test backend data retrieval with additional arguments."""
        inventory = self.TestInventory(self.mock_backend)

        mock_data = [{"id": "rec1", "Station": "KORD"}]
        self.mock_backend.read_records.return_value = mock_data

        result = inventory._get_backend_data(
            "stations", filter_by="status", value="active"
        )

        # Should pass kwargs to backend
        self.mock_backend.read_records.assert_called_once_with(
            "Stations", filter_by="status", value="active"
        )
        assert result == mock_data

    def test_get_backend_data_table_not_found(self):
        """Test error handling when table mapping is not found."""
        inventory = self.TestInventory(self.mock_backend)

        with pytest.raises(BackendError) as exc_info:
            inventory._get_backend_data("nonexistent_table")

        assert "Table mapping error" in str(exc_info.value)

    def test_get_backend_data_backend_error(self):
        """Test error handling when backend operation fails."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend to raise an exception
        self.mock_backend.read_records.side_effect = Exception(
            "Backend connection failed"
        )

        with pytest.raises(BackendError) as exc_info:
            inventory._get_backend_data("elements")

        assert "Failed to retrieve data from backend" in str(exc_info.value)
        assert "Backend connection failed" in str(exc_info.value)

    def test_transform_data_success(self):
        """Test successful data transformation."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock transformer
        mock_transformer = Mock()
        mock_transformer.transform.return_value = [
            {"element": "TEMP", "description": "Temperature"}
        ]

        raw_data = [{"id": "rec1", "Element": "TEMP", "Description": "Temperature"}]
        result = inventory._transform_data(mock_transformer, raw_data)

        mock_transformer.transform.assert_called_once_with(raw_data)
        assert result == [{"element": "TEMP", "description": "Temperature"}]

    def test_transform_data_error(self):
        """Test error handling during data transformation."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock transformer to raise an exception
        mock_transformer = Mock()
        mock_transformer.transform.side_effect = TransformationError("Invalid data")
        mock_transformer.__name__ = "MockTransformer"

        raw_data = [{"invalid": "data"}]

        with pytest.raises(TransformationError):
            inventory._transform_data(mock_transformer, raw_data)

    def test_apply_filters_no_filters(self):
        """Test applying no filters returns original data."""
        inventory = self.TestInventory(self.mock_backend)

        data = [
            {"element": "TEMP", "description": "Temperature"},
            {"element": "HUMID", "description": "Humidity"},
        ]

        result = inventory._apply_filters(data, None)
        assert result == data

        result = inventory._apply_filters(data, {})
        assert result == data

    def test_apply_filters_exact_match(self):
        """Test applying exact match filters."""
        inventory = self.TestInventory(self.mock_backend)

        data = [
            {"element": "TEMP", "status": "active"},
            {"element": "HUMID", "status": "inactive"},
            {"element": "PRESS", "status": "active"},
        ]

        filters = {"status": "active"}
        result = inventory._apply_filters(data, filters)

        expected = [
            {"element": "TEMP", "status": "active"},
            {"element": "PRESS", "status": "active"},
        ]
        assert result == expected

    def test_apply_filters_multiple_values(self):
        """Test applying filters with multiple values (OR condition)."""
        inventory = self.TestInventory(self.mock_backend)

        data = [
            {"element": "TEMP", "status": "active"},
            {"element": "HUMID", "status": "inactive"},
            {"element": "PRESS", "status": "pending"},
        ]

        filters = {"status": ["active", "pending"]}
        result = inventory._apply_filters(data, filters)

        expected = [
            {"element": "TEMP", "status": "active"},
            {"element": "PRESS", "status": "pending"},
        ]
        assert result == expected

    def test_apply_filters_complex_filters(self):
        """Test applying complex filters."""
        inventory = self.TestInventory(self.mock_backend)

        data = [
            {"element": "TEMP", "value": 25.5},
            {"element": "HUMID", "value": 45.0},
            {"element": "PRESS", "value": 1013.2},
        ]

        filters = {"value": {"min": 30, "max": 1000}}
        result = inventory._apply_filters(data, filters)

        expected = [{"element": "HUMID", "value": 45.0}]
        assert result == expected

    def test_evaluate_complex_filter_min_max(self):
        """Test complex filter evaluation with min/max."""
        inventory = self.TestInventory(self.mock_backend)

        # Test min/max filters
        assert inventory._evaluate_complex_filter(50, {"min": 10, "max": 100}) == True
        assert inventory._evaluate_complex_filter(5, {"min": 10, "max": 100}) == False
        assert inventory._evaluate_complex_filter(150, {"min": 10, "max": 100}) == False

    def test_evaluate_complex_filter_string_operations(self):
        """Test complex filter evaluation with string operations."""
        inventory = self.TestInventory(self.mock_backend)

        # Test contains
        assert (
            inventory._evaluate_complex_filter("temperature", {"contains": "temp"})
            == True
        )
        assert (
            inventory._evaluate_complex_filter("humidity", {"contains": "temp"})
            == False
        )

        # Test startswith
        assert (
            inventory._evaluate_complex_filter("temperature", {"startswith": "temp"})
            == True
        )
        assert (
            inventory._evaluate_complex_filter("humidity", {"startswith": "temp"})
            == False
        )

        # Test endswith
        assert (
            inventory._evaluate_complex_filter("temperature", {"endswith": "ture"})
            == True
        )
        assert (
            inventory._evaluate_complex_filter("humidity", {"endswith": "ture"})
            == False
        )


class TestNetworkSchemaMethodsIntegration:
    """Integration tests for all network schema methods."""

    def setup_method(self):
        """Set up test fixtures."""

        # Create a test class that inherits from NetworkSchemaMixin
        class TestInventory(NetworkSchemaMixin):
            def __init__(self, backend, **kwargs):
                self.backend = backend
                super().__init__(**kwargs)

        self.TestInventory = TestInventory
        self.mock_backend = MockInventoryBackend()

    def test_get_elements_success(self):
        """Test successful elements data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": "Celsius",
                "US Units": "Fahrenheit",
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_elements()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Elements")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["element"] == "TEMP"
        assert result[0]["description"] == "Air Temperature"
        assert result[0]["description_short"] == "Temp"
        assert result[0]["si_units"] == "Celsius"
        assert result[0]["us_units"] == "Fahrenheit"

    def test_get_elements_with_filters(self):
        """Test elements data retrieval with filters."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response with multiple elements
        mock_raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": "Celsius",
                "US Units": "Fahrenheit",
            },
            {
                "id": "rec2",
                "Element": "HUMID",
                "Description": "Relative Humidity",
                "Description Short": "RH",
                "SI Units": "%",
                "US Units": "%",
            },
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        # Apply filter
        filters = {"element": "TEMP"}
        result = inventory.get_elements(filters=filters)

        # Should return only filtered data
        assert len(result) == 1
        assert result[0]["element"] == "TEMP"

    def test_get_component_models_success(self):
        """Test successful component models data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Manufacturer": "Vaisala",
                "Type": "Weather Transmitter",
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_component_models()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Component Models")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["model"] == "WXT536"
        assert result[0]["manufacturer"] == "Vaisala"
        assert result[0]["type"] == "Weather Transmitter"

    def test_get_stations_success(self):
        """Test successful stations data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": "41.9742",
                "Longitude": "-87.9073",
                "Elevation": "201.5",
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_stations()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Stations")

        # Should return transformed data with proper type conversion
        assert len(result) == 1
        assert result[0]["station"] == "KORD"
        assert result[0]["name"] == "Chicago O'Hare International Airport"
        assert result[0]["status"] == "active"
        assert result[0]["latitude"] == 41.9742
        assert result[0]["longitude"] == -87.9073
        assert result[0]["elevation"] == 201.5

    def test_get_inventory_success(self):
        """Test successful inventory data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {"id": "rec1", "Model": "WXT536", "Serial Number": "WXT536001"}
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_inventory()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Inventory")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["model"] == "WXT536"
        assert result[0]["serial_number"] == "WXT536001"

    def test_get_deployments_success(self):
        """Test successful deployments data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
                "Date Assigned": "2023-01-15",
                "Elevation (cm)": "200",
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_deployments()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Deployments")

        # Should return transformed data with proper type conversion
        assert len(result) == 1
        assert result[0]["station"] == "KORD"
        assert result[0]["model"] == "WXT536"
        assert result[0]["serial_number"] == "WXT536001"
        assert result[0]["elevation_cm"] == 200

    def test_get_component_elements_success(self):
        """Test successful component elements data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Element": "TEMP",
                "QC Values": {"min": -40, "max": 60},
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_component_elements()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Component Elements")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["model"] == "WXT536"
        assert result[0]["element"] == "TEMP"
        assert result[0]["qc_values"] == {"min": -40, "max": 60}

    def test_get_request_schemas_success(self):
        """Test successful request schemas data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Network": "mesonet",
                "Request Model": {
                    "type": "object",
                    "properties": {"station": {"type": "string"}},
                },
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_request_schemas()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Request Schemas")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["network"] == "mesonet"
        assert result[0]["request_model"]["type"] == "object"

    def test_get_response_schemas_success(self):
        """Test successful response schemas data retrieval."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response
        mock_raw_data = [
            {
                "id": "rec1",
                "Response Name": "weather_data",
                "Response Model": {
                    "type": "object",
                    "properties": {"temperature": {"type": "number"}},
                },
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        result = inventory.get_response_schemas()

        # Should call backend with correct table name
        self.mock_backend.read_records.assert_called_once_with("Response Schemas")

        # Should return transformed data
        assert len(result) == 1
        assert result[0]["response_name"] == "weather_data"
        assert result[0]["response_model"]["type"] == "object"

    def test_backend_error_propagation(self):
        """Test that backend errors are properly propagated."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend to raise an exception
        self.mock_backend.read_records.side_effect = Exception(
            "Backend connection failed"
        )

        with pytest.raises(BackendError):
            inventory.get_elements()

    def test_transformation_error_propagation(self):
        """Test that transformation errors are properly propagated."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend response with invalid data that will cause transformation to fail
        mock_raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                # Missing required fields will cause transformation error
            }
        ]
        self.mock_backend.read_records.return_value = mock_raw_data

        with pytest.raises(TransformationError):
            inventory.get_elements()

    def test_all_methods_use_correct_table_names(self):
        """Test that all methods use the correct backend table names."""
        inventory = self.TestInventory(self.mock_backend)

        # Mock backend to return empty data
        self.mock_backend.read_records.return_value = []

        # Test each method and verify correct table name is used
        test_cases = [
            (inventory.get_elements, "Elements"),
            (inventory.get_component_models, "Component Models"),
            (inventory.get_stations, "Stations"),
            (inventory.get_inventory, "Inventory"),
            (inventory.get_deployments, "Deployments"),
            (inventory.get_component_elements, "Component Elements"),
            (inventory.get_request_schemas, "Request Schemas"),
            (inventory.get_response_schemas, "Response Schemas"),
        ]

        for method, expected_table_name in test_cases:
            self.mock_backend.read_records.reset_mock()
            method()
            self.mock_backend.read_records.assert_called_once_with(expected_table_name)
