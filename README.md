# mbx-inventory

No Code Database for Managing Mesonet-in-a-Box Deployments and Inventory. This library provides a flexible `Inventory` class that can be adapted to use whichever backend you prefer, along with a powerful CLI for synchronizing inventory data to PostgreSQL databases.

## Features

- **Multiple Backend Support**: AirTable, Baserow, and NocoDB
- **Command-Line Interface**: Easy-to-use CLI for data synchronization
- **PostgreSQL Integration**: Seamless sync to PostgreSQL databases
- **Flexible Configuration**: JSON-based configuration with environment variable support
- **Dry-Run Mode**: Preview changes before applying them
- **Selective Sync**: Sync specific tables or all tables
- **Progress Reporting**: Real-time progress updates and detailed logging
- **Error Handling**: Comprehensive error reporting and recovery

## Installation

```bash
pip install mbx-inventory
```

## Quick Start

### CLI Usage

1. **Create a configuration file**:
   ```bash
   cp examples/airtable_config.json inventory_config.json
   ```

2. **Set up environment variables**:
   ```bash
   export AIRTABLE_API_KEY="your_api_key"
   export AIRTABLE_BASE_ID="your_base_id"
   export POSTGRES_HOST="localhost"
   export POSTGRES_DB="your_database"
   export POSTGRES_USER="your_username"
   export POSTGRES_PASSWORD="your_password"
   ```

3. **Validate your configuration**:
   ```bash
   mbx-inventory validate
   ```

4. **Run a dry-run sync**:
   ```bash
   mbx-inventory sync --dry-run --verbose
   ```

5. **Perform the actual sync**:
   ```bash
   mbx-inventory sync
   ```

### Python Library Usage

```python
from mbx_inventory import Inventory
from mbx_inventory.backends import AirtableBackend

# Create backend instance
backend = AirtableBackend(api_key="your_key", base_id="your_base")

# Create inventory instance
inventory = Inventory(backend)

# Get data
elements = inventory.get_elements()
stations = inventory.get_stations()
```

## CLI Commands

### Main Commands

- `mbx-inventory validate` - Validate backend connection and configuration
- `mbx-inventory sync` - Sync inventory data from backend to PostgreSQL
- `mbx-inventory config show` - Display current configuration
- `mbx-inventory config validate` - Validate configuration file

### Common Options

- `--config, -c`: Specify configuration file path
- `--verbose, -v`: Enable verbose logging
- `--dry-run`: Preview changes without executing them
- `--tables`: Sync specific tables only

### Examples

```bash
# Validate configuration with connectivity test
mbx-inventory validate --verbose

# Sync specific tables only
mbx-inventory sync --tables elements,stations

# Dry-run with verbose output
mbx-inventory sync --dry-run --verbose

# Use custom configuration file
mbx-inventory sync --config production_config.json
```

## Configuration

The CLI uses JSON configuration files to define backend connections, database settings, and sync options.

### Example Configuration (AirTable)

```json
{
  "backend": {
    "type": "airtable",
    "config": {
      "api_key": "${AIRTABLE_API_KEY}",
      "base_id": "${AIRTABLE_BASE_ID}"
    }
  },
  "database": {
    "host": "${POSTGRES_HOST}",
    "port": 5432,
    "database": "${POSTGRES_DB}",
    "username": "${POSTGRES_USER}",
    "password": "${POSTGRES_PASSWORD}"
  },
  "table_mappings": {
    "elements": "Elements",
    "stations": "Stations",
    "component_models": "Component Model Elements",
    "inventory": "Component Inventory",
    "deployments": "Component Deployments"
  },
  "sync_options": {
    "batch_size": 100,
    "timeout": 30,
    "retry_attempts": 3
  }
}
```

## Supported Backends

### AirTable
- **Requirements**: API key and base ID
- **Example Base**: [Example AirTable Base](https://airtable.com/appasuz41SO5upfoa/shrHWLUAPB4wiOHY0)
- **Configuration**: See `examples/airtable_config.json`

### Baserow
- **Requirements**: API key, base URL, and database ID
- **Configuration**: See `examples/baserow_config.json`

### NocoDB
- **Requirements**: API key, base URL, and project ID
- **Configuration**: See `examples/nocodb_config.json`

## Available Tables

The following inventory tables can be synchronized:

- `elements` - Measurement elements and their properties
- `stations` - Weather station information
- `component_models` - Component model definitions
- `inventory` - Component inventory records
- `deployments` - Component deployment records
- `component_elements` - Component-element relationships
- `request_schemas` - API request schema definitions
- `response_schemas` - API response schema definitions

## Documentation

- **[CLI Usage Guide](docs/CLI_USAGE.md)** - Comprehensive CLI documentation
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Configuration Examples](examples/)** - Sample configuration files

## Integration with mbx-db

The `mbx-inventory` CLI works seamlessly with the `mbx-db` CLI:

1. **Set up database schema**:
   ```bash
   mbx-db create-schema --schema inventory
   ```

2. **Sync inventory data**:
   ```bash
   mbx-inventory sync
   ```

3. **Manage database**:
   ```bash
   mbx-db backup --schema inventory
   ```

## Development

### Installation for Development

```bash
git clone <repository-url>
cd mbx-inventory
pip install -e .
```

### Running Tests

```bash
pytest tests/ -v
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:

1. Check the [Troubleshooting Guide](docs/TROUBLESHOOTING.md)
2. Review the [CLI Usage Guide](docs/CLI_USAGE.md)
3. Look at configuration examples in the `examples/` directory
4. Open an issue on the repository
