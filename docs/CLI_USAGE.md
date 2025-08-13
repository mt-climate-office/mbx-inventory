# mbx-inventory CLI Usage Guide

The `mbx-inventory` CLI provides a command-line interface for synchronizing inventory data from various backends (AirTable, Baserow, NocoDB) to a PostgreSQL database.

## Installation

The CLI is installed automatically when you install the `mbx-inventory` package:

```bash
pip install mbx-inventory
```

After installation, the `mbx-inventory` command will be available in your PATH.

## Quick Start

1. **Create a configuration file** (see [Configuration](#configuration) section)
2. **Set up environment variables** (see [Environment Variables](#environment-variables) section)
3. **Validate your configuration**:
   ```bash
   mbx-inventory validate --config inventory_config.json
   ```
4. **Run a dry-run sync** to preview changes:
   ```bash
   mbx-inventory sync --dry-run --verbose
   ```
5. **Perform the actual sync**:
   ```bash
   mbx-inventory sync
   ```

## Commands

### Main Commands

#### `validate`
Validate backend connection and configuration.

```bash
mbx-inventory validate [OPTIONS]
```

**Options:**
- `--config, -c PATH`: Path to configuration file (default: `inventory_config.json`)
- `--verbose, -v`: Enable verbose logging

**Examples:**
```bash
# Validate default configuration
mbx-inventory validate

# Validate specific configuration with verbose output
mbx-inventory validate --config production_config.json --verbose
```

#### `sync`
Sync inventory data from backend to PostgreSQL.

```bash
mbx-inventory sync [OPTIONS]
```

**Options:**
- `--config, -c PATH`: Path to configuration file (default: `inventory_config.json`)
- `--dry-run`: Preview changes without executing them
- `--tables TEXT`: Comma-separated list of tables to sync (sync all if not specified)
- `--verbose, -v`: Enable verbose logging

**Examples:**
```bash
# Dry-run sync to preview changes
mbx-inventory sync --dry-run

# Sync specific tables only
mbx-inventory sync --tables elements,stations

# Full sync with verbose logging
mbx-inventory sync --verbose

# Sync with custom configuration
mbx-inventory sync --config production_config.json
```

### Configuration Commands

#### `config show`
Display current configuration in a formatted view.

```bash
mbx-inventory config show [OPTIONS]
```

**Options:**
- `--config, -c PATH`: Path to configuration file (default: `inventory_config.json`)

**Examples:**
```bash
# Show default configuration
mbx-inventory config show

# Show specific configuration
mbx-inventory config show --config production_config.json
```

#### `config validate`
Validate configuration file structure and settings.

```bash
mbx-inventory config validate [OPTIONS]
```

**Options:**
- `--config, -c PATH`: Path to configuration file (default: `inventory_config.json`)
- `--test-connectivity`: Test backend and database connectivity
- `--verbose, -v`: Enable verbose logging

**Examples:**
```bash
# Basic configuration validation
mbx-inventory config validate

# Validate with connectivity testing
mbx-inventory config validate --test-connectivity --verbose
```

## Configuration

The CLI uses a JSON configuration file to define backend connections, database settings, table mappings, and sync options.

### Configuration File Structure

```json
{
  "backend": {
    "type": "airtable|baserow|nocodb",
    "config": {
      // Backend-specific configuration
    }
  },
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "database_name",
    "username": "username",
    "password": "password"
  },
  "table_mappings": {
    "local_table_name": "backend_table_name"
  },
  "sync_options": {
    "batch_size": 100,
    "timeout": 30,
    "retry_attempts": 3
  }
}
```

### Backend-Specific Configuration

#### AirTable
```json
{
  "backend": {
    "type": "airtable",
    "config": {
      "api_key": "${AIRTABLE_API_KEY}",
      "base_id": "${AIRTABLE_BASE_ID}"
    }
  }
}
```

#### Baserow
```json
{
  "backend": {
    "type": "baserow",
    "config": {
      "api_key": "${BASEROW_API_KEY}",
      "base_url": "${BASEROW_BASE_URL}",
      "database_id": "${BASEROW_DATABASE_ID}"
    }
  }
}
```

#### NocoDB
```json
{
  "backend": {
    "type": "nocodb",
    "config": {
      "api_key": "${NOCODB_API_KEY}",
      "base_url": "${NOCODB_BASE_URL}",
      "project_id": "${NOCODB_PROJECT_ID}"
    }
  }
}
```

### Available Tables

The following tables are available for synchronization:

- `elements`: Measurement elements and their properties
- `stations`: Weather station information
- `component_models`: Component model definitions
- `inventory`: Component inventory records
- `deployments`: Component deployment records
- `component_elements`: Component-element relationships
- `request_schemas`: API request schema definitions
- `response_schemas`: API response schema definitions

## Environment Variables

The CLI supports environment variable substitution in configuration files using the `${VARIABLE_NAME}` syntax.

### Required Environment Variables

#### For AirTable:
- `AIRTABLE_API_KEY`: Your AirTable API key
- `AIRTABLE_BASE_ID`: Your AirTable base ID

#### For Baserow:
- `BASEROW_API_KEY`: Your Baserow API key
- `BASEROW_BASE_URL`: Baserow API base URL
- `BASEROW_DATABASE_ID`: Your Baserow database ID

#### For NocoDB:
- `NOCODB_API_KEY`: Your NocoDB API key
- `NOCODB_BASE_URL`: NocoDB instance base URL
- `NOCODB_PROJECT_ID`: Your NocoDB project ID

#### For PostgreSQL (all backends):
- `POSTGRES_HOST`: PostgreSQL server hostname
- `POSTGRES_PORT`: PostgreSQL server port (optional, defaults to 5432)
- `POSTGRES_DB`: PostgreSQL database name
- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password

### Setting Environment Variables

You can set environment variables in several ways:

1. **Using a `.env` file** (recommended):
   ```bash
   # Create a .env file in your project directory
   cp examples/.env.example .env
   # Edit the .env file with your actual values
   ```

2. **Export in your shell**:
   ```bash
   export AIRTABLE_API_KEY="your_api_key_here"
   export AIRTABLE_BASE_ID="your_base_id_here"
   # ... other variables
   ```

3. **Using environment management tools** like `direnv`, `conda`, or `virtualenv`.

## Examples

### Example 1: Basic AirTable Sync

1. Create configuration file `airtable_config.json`:
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
       "host": "localhost",
       "port": 5432,
       "database": "inventory_db",
       "username": "${POSTGRES_USER}",
       "password": "${POSTGRES_PASSWORD}"
     },
     "table_mappings": {
       "elements": "Elements",
       "stations": "Stations"
     }
   }
   ```

2. Set environment variables:
   ```bash
   export AIRTABLE_API_KEY="your_api_key"
   export AIRTABLE_BASE_ID="your_base_id"
   export POSTGRES_USER="your_username"
   export POSTGRES_PASSWORD="your_password"
   ```

3. Validate and sync:
   ```bash
   mbx-inventory validate --config airtable_config.json
   mbx-inventory sync --config airtable_config.json --dry-run
   mbx-inventory sync --config airtable_config.json
   ```

### Example 2: Selective Table Sync with Baserow

```bash
# Sync only elements and stations tables
mbx-inventory sync --config baserow_config.json --tables elements,stations --verbose
```

### Example 3: Production Deployment

```bash
# Validate production configuration
mbx-inventory config validate --config production_config.json --test-connectivity

# Run dry-run to preview changes
mbx-inventory sync --config production_config.json --dry-run --verbose

# Execute the sync
mbx-inventory sync --config production_config.json --verbose
```

## Troubleshooting

### Common Issues

#### Configuration Errors
- **Missing environment variables**: Ensure all required environment variables are set
- **Invalid JSON**: Validate your configuration file syntax using a JSON validator
- **Wrong backend type**: Ensure backend type is one of: `airtable`, `baserow`, `nocodb`

#### Connection Errors
- **Backend connection failed**: Check your API keys and base/database IDs
- **Database connection failed**: Verify PostgreSQL connection details and ensure the database is running
- **Network issues**: Check firewall settings and network connectivity

#### Sync Errors
- **Table not found**: Verify table names in your backend match the configuration
- **Schema mismatch**: Ensure your PostgreSQL schema is compatible with the inventory data
- **Permission errors**: Check that your API keys have sufficient permissions

### Getting Help

1. **Use verbose mode** for detailed logging:
   ```bash
   mbx-inventory sync --verbose
   ```

2. **Check configuration**:
   ```bash
   mbx-inventory config show
   mbx-inventory config validate --test-connectivity
   ```

3. **Run dry-run** to preview changes:
   ```bash
   mbx-inventory sync --dry-run --verbose
   ```

4. **Check the logs** for specific error messages and context.

### Exit Codes

- `0`: Success
- `1`: Configuration or validation error
- `2`: Command line argument error

## Integration with mbx-db

The `mbx-inventory` CLI is designed to work seamlessly with the `mbx-db` CLI tool:

1. **Use `mbx-db` to set up your PostgreSQL schema**:
   ```bash
   mbx-db create-schema --schema inventory
   ```

2. **Use `mbx-inventory` to populate the schema with data**:
   ```bash
   mbx-inventory sync --verbose
   ```

3. **Use `mbx-db` for database management tasks**:
   ```bash
   mbx-db backup --schema inventory
   mbx-db migrate --schema inventory
   ```

This integration provides a complete solution for managing your Mesonet-in-a-Box inventory data infrastructure.