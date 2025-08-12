"""
Unit tests for data transformers.
"""

import pytest
from datetime import date, datetime
from mbx_inventory.transformers import (
    BaseTransformer,
    NetworkSchemaError,
    ValidationError,
    TransformationError,
    BackendError,
)


class TestTransformer(BaseTransformer):
    """Test transformer for unit testing."""

    REQUIRED_FIELDS = ["name", "value"]
    OPTIONAL_FIELDS = ["description", "extra_data"]
    FIELD_MAPPINGS = {"name": "Name", "value": "Value", "description": "Description"}


class TestTransformerWithTypes(BaseTransformer):
    """Test transformer with field types defined."""

    REQUIRED_FIELDS = ["name", "count"]
    OPTIONAL_FIELDS = ["price", "created_date", "extra_data"]
    FIELD_MAPPINGS = {
        "name": "Name",
        "count": "Count",
        "price": "Price",
        "created_date": "Created Date",
    }
    FIELD_TYPES = {
        "count": int,
        "price": float,
        "created_date": date,
    }


class TestBaseTransformer:
    """Test cases for BaseTransformer class."""

    def test_transform_empty_data(self):
        """Test transforming empty data returns empty list."""
        result = TestTransformer.transform([])
        assert result == []

    def test_transform_valid_data(self):
        """Test transforming valid data."""
        raw_data = [
            {
                "id": "rec1",
                "Name": "Test Item",
                "Value": 42,
                "Description": "Test description",
            }
        ]

        result = TestTransformer.transform(raw_data)

        expected = [
            {"name": "Test Item", "value": 42, "description": "Test description"}
        ]

        assert result == expected

    def test_transform_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Name": "Test Item",
                # Missing 'Value' field
            }
        ]

        with pytest.raises(TransformationError) as exc_info:
            TestTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)

    def test_transform_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Name": "Test Item",
                "Value": 42,
                "ExtraField": "extra value",
                "AnotherExtra": 123,
            }
        ]

        result = TestTransformer.transform(raw_data)

        assert len(result) == 1
        assert result[0]["name"] == "Test Item"
        assert result[0]["value"] == 42
        assert "extra_data" in result[0]
        assert result[0]["extra_data"]["ExtraField"] == "extra value"
        assert result[0]["extra_data"]["AnotherExtra"] == 123

    def test_transform_optional_fields_none(self):
        """Test transformation handles None optional fields correctly."""
        raw_data = [
            {"id": "rec1", "Name": "Test Item", "Value": 42, "Description": None}
        ]

        result = TestTransformer.transform(raw_data)

        assert len(result) == 1
        assert "description" not in result[0]  # None values should be omitted

    def test_validate_required_fields_success(self):
        """Test successful validation of required fields."""
        data = {"name": "test", "value": 42}
        # Should not raise any exception
        BaseTransformer.validate_required_fields(data, ["name", "value"])

    def test_validate_required_fields_missing(self):
        """Test validation fails when required fields are missing."""
        data = {"name": "test"}

        with pytest.raises(ValidationError) as exc_info:
            BaseTransformer.validate_required_fields(data, ["name", "value"])

        assert "Missing required fields: value" in str(exc_info.value)

    def test_validate_required_fields_multiple_missing(self):
        """Test validation fails when multiple required fields are missing."""
        data = {}

        with pytest.raises(ValidationError) as exc_info:
            BaseTransformer.validate_required_fields(data, ["name", "value"])

        error_msg = str(exc_info.value)
        assert "Missing required fields:" in error_msg
        assert "name" in error_msg
        assert "value" in error_msg

    def test_convert_value_date_from_date(self):
        """Test date conversion from date object."""
        test_date = date(2023, 12, 25)
        result = BaseTransformer.convert_value(test_date, date)
        assert result == test_date

    def test_convert_value_date_from_datetime(self):
        """Test date conversion from datetime object."""
        test_datetime = datetime(2023, 12, 25, 10, 30, 0)
        result = BaseTransformer.convert_value(test_datetime, date)
        assert result == date(2023, 12, 25)

    def test_convert_value_date_from_string(self):
        """Test date conversion from string."""
        result = BaseTransformer.convert_value("2023-12-25", date)
        assert result == date(2023, 12, 25)

        result = BaseTransformer.convert_value("12/25/2023", date)
        assert result == date(2023, 12, 25)

    def test_convert_value_date_invalid(self):
        """Test date conversion with invalid input."""
        result = BaseTransformer.convert_value("invalid-date", date)
        assert result is None

        result = BaseTransformer.convert_value(None, date)
        assert result is None

    def test_convert_value_float_valid(self):
        """Test float conversion with valid inputs."""
        assert BaseTransformer.convert_value(42, float) == 42.0
        assert BaseTransformer.convert_value("42.5", float) == 42.5
        assert BaseTransformer.convert_value(42.5, float) == 42.5

    def test_convert_value_float_invalid(self):
        """Test float conversion with invalid inputs."""
        assert BaseTransformer.convert_value("invalid", float) is None
        assert BaseTransformer.convert_value(None, float) is None

    def test_convert_value_int_valid(self):
        """Test int conversion with valid inputs."""
        assert BaseTransformer.convert_value(42, int) == 42
        assert BaseTransformer.convert_value("42", int) == 42
        assert BaseTransformer.convert_value(42.0, int) == 42

    def test_convert_value_int_invalid(self):
        """Test int conversion with invalid inputs."""
        assert BaseTransformer.convert_value("invalid", int) is None
        assert BaseTransformer.convert_value(None, int) is None

    def test_convert_value_string(self):
        """Test string conversion."""
        assert BaseTransformer.convert_value(42, str) == "42"
        assert BaseTransformer.convert_value(42.5, str) == "42.5"
        assert BaseTransformer.convert_value("test", str) == "test"
        assert BaseTransformer.convert_value(None, str) is None

    def test_field_type_conversion(self):
        """Test automatic field type conversion using FIELD_TYPES."""
        raw_data = [
            {
                "id": "rec1",
                "Name": "Test Product",
                "Count": "42",  # String that should convert to int
                "Price": "19.99",  # String that should convert to float
                "Created Date": "2023-12-25",  # String that should convert to date
            }
        ]

        result = TestTransformerWithTypes.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["name"] == "Test Product"
        assert record["count"] == 42  # Converted to int
        assert record["price"] == 19.99  # Converted to float
        assert record["created_date"] == date(2023, 12, 25)  # Converted to date


class TestExceptionHierarchy:
    """Test the exception hierarchy."""

    def test_network_schema_error_base(self):
        """Test NetworkSchemaError is base exception."""
        error = NetworkSchemaError("test error")
        assert str(error) == "test error"
        assert isinstance(error, Exception)

    def test_validation_error_inheritance(self):
        """Test ValidationError inherits from NetworkSchemaError."""
        error = ValidationError("validation failed")
        assert isinstance(error, NetworkSchemaError)
        assert isinstance(error, Exception)

    def test_transformation_error_inheritance(self):
        """Test TransformationError inherits from NetworkSchemaError."""
        error = TransformationError("transformation failed")
        assert isinstance(error, NetworkSchemaError)
        assert isinstance(error, Exception)

    def test_backend_error_inheritance(self):
        """Test BackendError inherits from NetworkSchemaError."""
        error = BackendError("backend failed")
        assert isinstance(error, NetworkSchemaError)
        assert isinstance(error, Exception)


class TestElementsTransformer:
    """Test cases for ElementsTransformer class."""

    def test_transform_valid_elements_data(self):
        """Test transforming valid elements data."""
        raw_data = [
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

        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform(raw_data)

        expected = [
            {
                "element": "TEMP",
                "description": "Air Temperature",
                "description_short": "Temp",
                "si_units": "Celsius",
                "us_units": "Fahrenheit",
            },
            {
                "element": "HUMID",
                "description": "Relative Humidity",
                "description_short": "RH",
                "si_units": "%",
                "us_units": "%",
            },
        ]

        assert result == expected

    def test_transform_elements_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                # Missing "Description Short"
                "SI Units": "Celsius",
            }
        ]

        from mbx_inventory.transformers import ElementsTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            ElementsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "description_short" in str(exc_info.value)

    def test_transform_elements_with_optional_fields_missing(self):
        """Test transformation works when optional fields are missing."""
        raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                # Missing SI Units and US Units - should be OK
            }
        ]

        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform(raw_data)

        expected = [
            {
                "element": "TEMP",
                "description": "Air Temperature",
                "description_short": "Temp",
            }
        ]

        assert result == expected

    def test_transform_elements_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": "Celsius",
                "US Units": "Fahrenheit",
                "Measurement Range": "-40 to 60",
                "Accuracy": "±0.1°C",
                "Notes": "Primary temperature sensor",
            }
        ]

        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["element"] == "TEMP"
        assert record["description"] == "Air Temperature"
        assert record["description_short"] == "Temp"
        assert record["si_units"] == "Celsius"
        assert record["us_units"] == "Fahrenheit"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Measurement Range"] == "-40 to 60"
        assert extra_data["Accuracy"] == "±0.1°C"
        assert extra_data["Notes"] == "Primary temperature sensor"

    def test_transform_elements_with_none_optional_fields(self):
        """Test transformation handles None values in optional fields."""
        raw_data = [
            {
                "id": "rec1",
                "Element": "TEMP",
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": None,  # None should be omitted
                "US Units": "Fahrenheit",
            }
        ]

        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform(raw_data)

        expected = [
            {
                "element": "TEMP",
                "description": "Air Temperature",
                "description_short": "Temp",
                "us_units": "Fahrenheit",
                # si_units should not be present
            }
        ]

        assert result == expected
        assert "si_units" not in result[0]

    def test_transform_elements_empty_data(self):
        """Test transforming empty elements data."""
        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform([])
        assert result == []

    def test_elements_field_type_conversion(self):
        """Test that string fields are properly converted."""
        raw_data = [
            {
                "id": "rec1",
                "Element": 123,  # Non-string that should convert to string
                "Description": "Air Temperature",
                "Description Short": "Temp",
                "SI Units": 456,  # Non-string that should convert to string
            }
        ]

        from mbx_inventory.transformers import ElementsTransformer

        result = ElementsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["element"] == "123"  # Converted to string
        assert record["si_units"] == "456"  # Converted to string
        assert isinstance(record["element"], str)
        assert isinstance(record["si_units"], str)


class TestStationsTransformer:
    """Test cases for StationsTransformer class."""

    def test_transform_valid_stations_data(self):
        """Test transforming valid stations data."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": 41.9742,
                "Longitude": -87.9073,
                "Elevation": 201.5,
                "Date Installed": "2020-01-15",
            },
            {
                "id": "rec2",
                "Station": "KMDW",
                "Name": "Chicago Midway International Airport",
                "Status": "active",
                "Latitude": 41.7868,
                "Longitude": -87.7522,
                "Elevation": 188.4,
            },
        ]

        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform(raw_data)

        expected = [
            {
                "station": "KORD",
                "name": "Chicago O'Hare International Airport",
                "status": "active",
                "latitude": 41.9742,
                "longitude": -87.9073,
                "elevation": 201.5,
                "date_installed": date(2020, 1, 15),
            },
            {
                "station": "KMDW",
                "name": "Chicago Midway International Airport",
                "status": "active",
                "latitude": 41.7868,
                "longitude": -87.7522,
                "elevation": 188.4,
            },
        ]

        assert result == expected

    def test_transform_stations_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": 41.9742,
                "Longitude": -87.9073,
                # Missing elevation
            }
        ]

        from mbx_inventory.transformers import StationsTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            StationsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "elevation" in str(exc_info.value)

    def test_transform_stations_invalid_status(self):
        """Test transformation fails with invalid status value."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "invalid_status",  # Invalid status
                "Latitude": 41.9742,
                "Longitude": -87.9073,
                "Elevation": 201.5,
            }
        ]

        from mbx_inventory.transformers import StationsTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            StationsTransformer.transform(raw_data)

        assert "Invalid status value" in str(exc_info.value)
        assert "invalid_status" in str(exc_info.value)
        assert "pending" in str(exc_info.value)  # Should list valid options

    def test_transform_stations_valid_statuses(self):
        """Test transformation works with all valid status values."""
        valid_statuses = ["pending", "active", "decommissioned", "inactive"]

        raw_data = []
        for i, status in enumerate(valid_statuses):
            raw_data.append(
                {
                    "id": f"rec{i + 1}",
                    "Station": f"STAT{i + 1}",
                    "Name": f"Station {i + 1}",
                    "Status": status,
                    "Latitude": 40.0 + i,
                    "Longitude": -90.0 - i,
                    "Elevation": 100.0 + i * 10,
                }
            )

        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform(raw_data)

        assert len(result) == 4
        for i, record in enumerate(result):
            assert record["status"] == valid_statuses[i]

    def test_transform_stations_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": 41.9742,
                "Longitude": -87.9073,
                "Elevation": 201.5,
                "Owner": "City of Chicago",
                "Contact": "john.doe@chicago.gov",
                "Notes": "Primary weather station for Chicago area",
            }
        ]

        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["station"] == "KORD"
        assert record["name"] == "Chicago O'Hare International Airport"
        assert record["status"] == "active"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Owner"] == "City of Chicago"
        assert extra_data["Contact"] == "john.doe@chicago.gov"
        assert extra_data["Notes"] == "Primary weather station for Chicago area"

    def test_transform_stations_coordinate_conversion(self):
        """Test that coordinate values are properly converted to float."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": "41.9742",  # String that should convert to float
                "Longitude": "-87.9073",  # String that should convert to float
                "Elevation": "201.5",  # String that should convert to float
            }
        ]

        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["latitude"] == 41.9742
        assert record["longitude"] == -87.9073
        assert record["elevation"] == 201.5
        assert isinstance(record["latitude"], float)
        assert isinstance(record["longitude"], float)
        assert isinstance(record["elevation"], float)

    def test_transform_stations_date_conversion(self):
        """Test that date_installed is properly converted to date object."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": 41.9742,
                "Longitude": -87.9073,
                "Elevation": 201.5,
                "Date Installed": "2020-01-15",  # String date
            }
        ]

        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["date_installed"] == date(2020, 1, 15)
        assert isinstance(record["date_installed"], date)

    def test_transform_stations_empty_data(self):
        """Test transforming empty stations data."""
        from mbx_inventory.transformers import StationsTransformer

        result = StationsTransformer.transform([])
        assert result == []

    def test_transform_stations_invalid_coordinates(self):
        """Test handling of invalid coordinate values."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Name": "Chicago O'Hare International Airport",
                "Status": "active",
                "Latitude": "invalid_lat",  # Invalid latitude
                "Longitude": -87.9073,
                "Elevation": 201.5,
            }
        ]

        from mbx_inventory.transformers import StationsTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            StationsTransformer.transform(raw_data)

        assert "Could not convert required field 'latitude'" in str(exc_info.value)


class TestComponentModelsTransformer:
    """Test cases for ComponentModelsTransformer class."""

    def test_transform_valid_component_models_data(self):
        """Test transforming valid component models data."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Manufacturer": "Vaisala",
                "Type": "Weather Transmitter",
            },
            {
                "id": "rec2",
                "Model": "CS215",
                "Manufacturer": "Campbell Scientific",
                "Type": "Temperature and Humidity Sensor",
            },
        ]

        from mbx_inventory.transformers import ComponentModelsTransformer

        result = ComponentModelsTransformer.transform(raw_data)

        expected = [
            {
                "model": "WXT536",
                "manufacturer": "Vaisala",
                "type": "Weather Transmitter",
            },
            {
                "model": "CS215",
                "manufacturer": "Campbell Scientific",
                "type": "Temperature and Humidity Sensor",
            },
        ]

        assert result == expected

    def test_transform_component_models_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Manufacturer": "Vaisala",
                # Missing "Type"
            }
        ]

        from mbx_inventory.transformers import (
            ComponentModelsTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            ComponentModelsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "type" in str(exc_info.value)

    def test_transform_component_models_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Manufacturer": "Vaisala",
                "Type": "Weather Transmitter",
                "Description": "All-in-one weather sensor",
                "Power Requirements": "12V DC",
                "Operating Temperature": "-52°C to +60°C",
                "Warranty": "2 years",
            }
        ]

        from mbx_inventory.transformers import ComponentModelsTransformer

        result = ComponentModelsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["model"] == "WXT536"
        assert record["manufacturer"] == "Vaisala"
        assert record["type"] == "Weather Transmitter"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Description"] == "All-in-one weather sensor"
        assert extra_data["Power Requirements"] == "12V DC"
        assert extra_data["Operating Temperature"] == "-52°C to +60°C"
        assert extra_data["Warranty"] == "2 years"

    def test_transform_component_models_type_conversion(self):
        """Test that fields are properly converted to strings."""
        raw_data = [
            {
                "id": "rec1",
                "Model": 12345,  # Non-string that should convert to string
                "Manufacturer": "Vaisala",
                "Type": "Weather Transmitter",
            }
        ]

        from mbx_inventory.transformers import ComponentModelsTransformer

        result = ComponentModelsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["model"] == "12345"  # Converted to string
        assert isinstance(record["model"], str)

    def test_transform_component_models_empty_data(self):
        """Test transforming empty component models data."""
        from mbx_inventory.transformers import ComponentModelsTransformer

        result = ComponentModelsTransformer.transform([])
        assert result == []

    def test_transform_component_models_with_none_values(self):
        """Test transformation handles None values correctly."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Manufacturer": None,  # None value for required field should fail
                "Type": "Weather Transmitter",
            }
        ]

        from mbx_inventory.transformers import (
            ComponentModelsTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            ComponentModelsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "manufacturer" in str(exc_info.value)


class TestInventoryTransformer:
    """Test cases for InventoryTransformer class."""

    def test_transform_valid_inventory_data(self):
        """Test transforming valid inventory data."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
            },
            {
                "id": "rec2",
                "Model": "CS215",
                "Serial Number": "CS215-042",
            },
        ]

        from mbx_inventory.transformers import InventoryTransformer

        result = InventoryTransformer.transform(raw_data)

        expected = [
            {
                "model": "WXT536",
                "serial_number": "WXT536001",
            },
            {
                "model": "CS215",
                "serial_number": "CS215-042",
            },
        ]

        assert result == expected

    def test_transform_inventory_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                # Missing "Serial Number"
            }
        ]

        from mbx_inventory.transformers import InventoryTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            InventoryTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "serial_number" in str(exc_info.value)

    def test_transform_inventory_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
                "Purchase Date": "2023-01-15",
                "Vendor": "Campbell Scientific",
                "Cost": "$2,500.00",
                "Warranty Expiry": "2025-01-15",
                "Location": "Warehouse A",
                "Condition": "New",
            }
        ]

        from mbx_inventory.transformers import InventoryTransformer

        result = InventoryTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["model"] == "WXT536"
        assert record["serial_number"] == "WXT536001"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Purchase Date"] == "2023-01-15"
        assert extra_data["Vendor"] == "Campbell Scientific"
        assert extra_data["Cost"] == "$2,500.00"
        assert extra_data["Warranty Expiry"] == "2025-01-15"
        assert extra_data["Location"] == "Warehouse A"
        assert extra_data["Condition"] == "New"

    def test_transform_inventory_type_conversion(self):
        """Test that fields are properly converted to strings."""
        raw_data = [
            {
                "id": "rec1",
                "Model": 12345,  # Non-string that should convert to string
                "Serial Number": 67890,  # Non-string that should convert to string
            }
        ]

        from mbx_inventory.transformers import InventoryTransformer

        result = InventoryTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["model"] == "12345"  # Converted to string
        assert record["serial_number"] == "67890"  # Converted to string
        assert isinstance(record["model"], str)
        assert isinstance(record["serial_number"], str)

    def test_transform_inventory_empty_data(self):
        """Test transforming empty inventory data."""
        from mbx_inventory.transformers import InventoryTransformer

        result = InventoryTransformer.transform([])
        assert result == []

    def test_transform_inventory_with_none_values(self):
        """Test transformation handles None values correctly."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Serial Number": None,  # None value for required field should fail
            }
        ]

        from mbx_inventory.transformers import InventoryTransformer, TransformationError

        with pytest.raises(TransformationError) as exc_info:
            InventoryTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "serial_number" in str(exc_info.value)

    def test_transform_inventory_minimal_data(self):
        """Test transformation with only required fields."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
            }
        ]

        from mbx_inventory.transformers import InventoryTransformer

        result = InventoryTransformer.transform(raw_data)

        expected = [
            {
                "model": "WXT536",
                "serial_number": "WXT536001",
            }
        ]

        assert result == expected
        # Should not have extra_data field when no extra fields present
        assert "extra_data" not in result[0]


class TestDeploymentsTransformer:
    """Test cases for DeploymentsTransformer class."""

    def test_transform_valid_deployments_data(self):
        """Test transforming valid deployments data."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
                "Date Assigned": "2023-01-15",
                "Date Start": "2023-01-20",
                "Date End": "2023-12-31",
                "Elevation (cm)": "200",
            },
            {
                "id": "rec2",
                "Station": "KMDW",
                "Model": "CS215",
                "Serial Number": "CS215-042",
                "Date Assigned": "2023-02-01",
                "Date Start": "2023-02-05",
                "Elevation (cm)": "150",
            },
        ]

        from mbx_inventory.transformers import DeploymentsTransformer

        result = DeploymentsTransformer.transform(raw_data)

        expected = [
            {
                "station": "KORD",
                "model": "WXT536",
                "serial_number": "WXT536001",
                "date_assigned": date(2023, 1, 15),
                "date_start": date(2023, 1, 20),
                "date_end": date(2023, 12, 31),
                "elevation_cm": 200,
            },
            {
                "station": "KMDW",
                "model": "CS215",
                "serial_number": "CS215-042",
                "date_assigned": date(2023, 2, 1),
                "date_start": date(2023, 2, 5),
                "elevation_cm": 150,
            },
        ]

        assert result == expected

    def test_transform_deployments_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
                # Missing "Date Assigned"
            }
        ]

        from mbx_inventory.transformers import (
            DeploymentsTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            DeploymentsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "date_assigned" in str(exc_info.value)

    def test_transform_deployments_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Station": "KORD",
                "Model": "WXT536",
                "Serial Number": "WXT536001",
                "Date Assigned": "2023-01-15",
                "Technician": "John Doe",
                "Installation Notes": "Installed on tower",
                "Maintenance Schedule": "Monthly",
            }
        ]

        from mbx_inventory.transformers import DeploymentsTransformer

        result = DeploymentsTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["station"] == "KORD"
        assert record["model"] == "WXT536"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Technician"] == "John Doe"
        assert extra_data["Installation Notes"] == "Installed on tower"
        assert extra_data["Maintenance Schedule"] == "Monthly"


class TestComponentElementsTransformer:
    """Test cases for ComponentElementsTransformer class."""

    def test_transform_valid_component_elements_data(self):
        """Test transforming valid component-element relationships data."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Element": "TEMP",
                "QC Values": {"min": -40, "max": 60, "precision": 0.1},
            },
            {
                "id": "rec2",
                "Model": "WXT536",
                "Element": "HUMID",
                "QC Values": {"min": 0, "max": 100, "precision": 0.1},
            },
        ]

        from mbx_inventory.transformers import ComponentElementsTransformer

        result = ComponentElementsTransformer.transform(raw_data)

        expected = [
            {
                "model": "WXT536",
                "element": "TEMP",
                "qc_values": {"min": -40, "max": 60, "precision": 0.1},
            },
            {
                "model": "WXT536",
                "element": "HUMID",
                "qc_values": {"min": 0, "max": 100, "precision": 0.1},
            },
        ]

        assert result == expected

    def test_transform_component_elements_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                # Missing "Element"
                "QC Values": {"min": -40, "max": 60},
            }
        ]

        from mbx_inventory.transformers import (
            ComponentElementsTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            ComponentElementsTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "element" in str(exc_info.value)

    def test_transform_component_elements_without_qc_values(self):
        """Test transformation works without optional QC values."""
        raw_data = [
            {
                "id": "rec1",
                "Model": "WXT536",
                "Element": "TEMP",
                # No QC Values - should be OK
            }
        ]

        from mbx_inventory.transformers import ComponentElementsTransformer

        result = ComponentElementsTransformer.transform(raw_data)

        expected = [
            {
                "model": "WXT536",
                "element": "TEMP",
            }
        ]

        assert result == expected


class TestRequestSchemasTransformer:
    """Test cases for RequestSchemasTransformer class."""

    def test_transform_valid_request_schemas_data(self):
        """Test transforming valid request schemas data."""
        raw_data = [
            {
                "id": "rec1",
                "Network": "mesonet",
                "Request Model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "elements": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
            {
                "id": "rec2",
                "Network": "synoptic",
                "Request Model": {
                    "type": "object",
                    "properties": {
                        "stid": {"type": "string"},
                        "vars": {"type": "string"},
                    },
                },
            },
        ]

        from mbx_inventory.transformers import RequestSchemasTransformer

        result = RequestSchemasTransformer.transform(raw_data)

        expected = [
            {
                "network": "mesonet",
                "request_model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "elements": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
            {
                "network": "synoptic",
                "request_model": {
                    "type": "object",
                    "properties": {
                        "stid": {"type": "string"},
                        "vars": {"type": "string"},
                    },
                },
            },
        ]

        assert result == expected

    def test_transform_request_schemas_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Network": "mesonet",
                # Missing "Request Model"
            }
        ]

        from mbx_inventory.transformers import (
            RequestSchemasTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            RequestSchemasTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "request_model" in str(exc_info.value)


class TestResponseSchemasTransformer:
    """Test cases for ResponseSchemasTransformer class."""

    def test_transform_valid_response_schemas_data(self):
        """Test transforming valid response schemas data."""
        raw_data = [
            {
                "id": "rec1",
                "Response Name": "weather_data",
                "Response Model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "temperature": {"type": "number"},
                        "humidity": {"type": "number"},
                    },
                },
            },
            {
                "id": "rec2",
                "Response Name": "station_metadata",
                "Response Model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "name": {"type": "string"},
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"},
                    },
                },
            },
        ]

        from mbx_inventory.transformers import ResponseSchemasTransformer

        result = ResponseSchemasTransformer.transform(raw_data)

        expected = [
            {
                "response_name": "weather_data",
                "response_model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "temperature": {"type": "number"},
                        "humidity": {"type": "number"},
                    },
                },
            },
            {
                "response_name": "station_metadata",
                "response_model": {
                    "type": "object",
                    "properties": {
                        "station": {"type": "string"},
                        "name": {"type": "string"},
                        "latitude": {"type": "number"},
                        "longitude": {"type": "number"},
                    },
                },
            },
        ]

        assert result == expected

    def test_transform_response_schemas_missing_required_field(self):
        """Test transformation fails when required field is missing."""
        raw_data = [
            {
                "id": "rec1",
                "Response Name": "weather_data",
                # Missing "Response Model"
            }
        ]

        from mbx_inventory.transformers import (
            ResponseSchemasTransformer,
            TransformationError,
        )

        with pytest.raises(TransformationError) as exc_info:
            ResponseSchemasTransformer.transform(raw_data)

        assert "Required fields not found" in str(exc_info.value)
        assert "response_model" in str(exc_info.value)

    def test_transform_response_schemas_with_extra_fields(self):
        """Test transformation includes extra fields in extra_data."""
        raw_data = [
            {
                "id": "rec1",
                "Response Name": "weather_data",
                "Response Model": {"type": "object"},
                "Version": "1.0",
                "Description": "Standard weather data response",
                "Documentation URL": "https://example.com/docs",
            }
        ]

        from mbx_inventory.transformers import ResponseSchemasTransformer

        result = ResponseSchemasTransformer.transform(raw_data)

        assert len(result) == 1
        record = result[0]
        assert record["response_name"] == "weather_data"

        # Check extra fields are captured
        assert "extra_data" in record
        extra_data = record["extra_data"]
        assert extra_data["Version"] == "1.0"
        assert extra_data["Description"] == "Standard weather data response"
        assert extra_data["Documentation URL"] == "https://example.com/docs"


class TestTableNameMapper:
    """Test cases for TableNameMapper class."""

    def test_default_airtable_mappings(self):
        """Test default AirTable mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="airtable")

        # Test some key mappings
        assert mapper.get_backend_table_name("elements") == "Elements"
        assert mapper.get_backend_table_name("component_models") == "Component Models"
        assert mapper.get_backend_table_name("stations") == "Stations"
        assert mapper.get_backend_table_name("inventory") == "Inventory"
        assert mapper.get_backend_table_name("deployments") == "Deployments"
        assert (
            mapper.get_backend_table_name("component_elements") == "Component Elements"
        )
        assert mapper.get_backend_table_name("request_schemas") == "Request Schemas"
        assert mapper.get_backend_table_name("response_schemas") == "Response Schemas"

    def test_default_baserow_mappings(self):
        """Test default Baserow mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="baserow")

        # Baserow uses snake_case
        assert mapper.get_backend_table_name("elements") == "Elements"
        assert mapper.get_backend_table_name("component_models") == "Component Models"
        assert mapper.get_backend_table_name("stations") == "Stations"

    def test_default_nocodb_mappings(self):
        """Test default NocoDB mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="nocodb")

        # NocoDB uses PascalCase without spaces
        assert mapper.get_backend_table_name("elements") == "Elements"
        assert mapper.get_backend_table_name("component_models") == "Component Models"
        assert (
            mapper.get_backend_table_name("component_elements") == "Component Elements"
        )

    def test_custom_mappings_override_defaults(self):
        """Test that custom mappings override default mappings."""
        from mbx_inventory.transformers import TableNameMapper

        custom_mappings = {
            "elements": "Sensor Elements",
            "stations": "Weather Stations",
        }

        mapper = TableNameMapper(
            custom_mappings=custom_mappings, backend_type="airtable"
        )

        # Custom mappings should override defaults
        assert mapper.get_backend_table_name("elements") == "Sensor Elements"
        assert mapper.get_backend_table_name("stations") == "Weather Stations"

        # Non-overridden mappings should use defaults
        assert mapper.get_backend_table_name("inventory") == "Inventory"

    def test_reverse_mapping(self):
        """Test getting schema table names from backend table names."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="airtable")

        # Test reverse mapping
        assert mapper.get_schema_table_name("Elements") == "elements"
        assert mapper.get_schema_table_name("Component Models") == "component_models"
        assert mapper.get_schema_table_name("Stations") == "stations"

    def test_get_backend_table_name_not_found(self):
        """Test error handling when schema table name is not found."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper()

        with pytest.raises(KeyError) as exc_info:
            mapper.get_backend_table_name("nonexistent_table")

        assert "No mapping found for schema table 'nonexistent_table'" in str(
            exc_info.value
        )
        assert "Available mappings:" in str(exc_info.value)

    def test_get_schema_table_name_not_found(self):
        """Test error handling when backend table name is not found."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper()

        with pytest.raises(KeyError) as exc_info:
            mapper.get_schema_table_name("Nonexistent Table")

        assert "No mapping found for backend table 'Nonexistent Table'" in str(
            exc_info.value
        )
        assert "Available backend tables:" in str(exc_info.value)

    def test_get_all_mappings(self):
        """Test getting all current mappings."""
        from mbx_inventory.transformers import TableNameMapper

        custom_mappings = {"elements": "Sensor Elements"}
        mapper = TableNameMapper(
            custom_mappings=custom_mappings, backend_type="airtable"
        )

        all_mappings = mapper.get_all_mappings()

        # Should include custom mapping
        assert all_mappings["elements"] == "Sensor Elements"

        # Should include default mappings
        assert all_mappings["stations"] == "Stations"

        # Should be a copy (not the original)
        all_mappings["test"] = "test"
        assert "test" not in mapper.get_all_mappings()

    def test_add_mapping(self):
        """Test adding new mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper()

        # Add new mapping
        mapper.add_mapping("custom_table", "Custom Table")

        assert mapper.get_backend_table_name("custom_table") == "Custom Table"
        assert mapper.get_schema_table_name("Custom Table") == "custom_table"

    def test_update_existing_mapping(self):
        """Test updating existing mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="airtable")

        # Original mapping
        assert mapper.get_backend_table_name("elements") == "Elements"

        # Update mapping
        mapper.add_mapping("elements", "Sensor Elements")

        # Should use new mapping
        assert mapper.get_backend_table_name("elements") == "Sensor Elements"

    def test_remove_mapping(self):
        """Test removing mappings."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper()

        # Verify mapping exists
        assert mapper.get_backend_table_name("elements") == "Elements"

        # Remove mapping
        mapper.remove_mapping("elements")

        # Should no longer exist
        with pytest.raises(KeyError):
            mapper.get_backend_table_name("elements")

    def test_remove_nonexistent_mapping(self):
        """Test error handling when removing nonexistent mapping."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper()

        with pytest.raises(KeyError) as exc_info:
            mapper.remove_mapping("nonexistent_table")

        assert "No mapping found for schema table 'nonexistent_table'" in str(
            exc_info.value
        )

    def test_case_insensitive_backend_type(self):
        """Test that backend type is case insensitive."""
        from mbx_inventory.transformers import TableNameMapper

        mapper1 = TableNameMapper(backend_type="AIRTABLE")
        mapper2 = TableNameMapper(backend_type="AirTable")
        mapper3 = TableNameMapper(backend_type="airtable")

        # All should produce the same mappings
        assert mapper1.get_backend_table_name("elements") == "Elements"
        assert mapper2.get_backend_table_name("elements") == "Elements"
        assert mapper3.get_backend_table_name("elements") == "Elements"

    def test_unknown_backend_type_uses_defaults(self):
        """Test that unknown backend types fall back to defaults."""
        from mbx_inventory.transformers import TableNameMapper

        mapper = TableNameMapper(backend_type="unknown_backend")

        # Should use default mappings
        assert mapper.get_backend_table_name("elements") == "Elements"
        assert mapper.get_backend_table_name("component_models") == "Component Models"
