# mbx-inventory Troubleshooting Guide

This guide helps you diagnose and resolve common issues with the mbx-inventory CLI.

## Quick Diagnostics

Before diving into specific issues, run these commands to get a quick overview of your setup:

```bash
# Check CLI installation
mbx-inventory --help

# Validate configuration
mbx-inventory config validate --verbose

# Test connectivity
mbx-inventory validate --verbose

# Preview sync without changes
mbx-inventory sync --dry-run --verbose
```

## Common Issues and Solutions

### Configuration Issues

#### Issue: "Configuration file not found"
```
Error: Configuration file not found: inventory_config.json
```

**Solution:**
1. Create a configuration file or specify the correct path:
   ```bash
   mbx-inventory sync --config /path/to/your/config.json
   ```
2. Use example configurations from the `examples/` directory:
   ```bash
   cp examples/airtable_config.json inventory_config.json
   ```

#### Issue: "Missing environment variable"
```
Error: Missing environment variable: AIRTABLE_API_KEY
```

**Solution:**
1. Set the required environment variable:
   ```bash
   export AIRTABLE_API_KEY="your_api_key_here"
   ```
2. Create a `.env` file:
   ```bash
   cp examples/.env.example .env
   # Edit .env with your actual values
   ```
3. Check which variables are required for your backend type:
   ```bash
   mbx-inventory config validate --verbose
   ```

#### Issue: "Invalid JSON in configuration file"
```
Error: Invalid JSON in configuration file: Expecting ',' delimiter: line 10 column 5
```

**Solution:**
1. Validate your JSON syntax using an online JSON validator
2. Common JSON errors:
   - Missing commas between objects
   - Trailing commas after the last item
   - Unmatched brackets or braces
   - Unescaped quotes in strings

#### Issue: "Backend type must be one of: airtable, baserow, nocodb"
```
Error: Backend type must be one of: ['airtable', 'baserow', 'nocodb']
```

**Solution:**
1. Check the `backend.type` field in your configuration:
   ```json
   {
     "backend": {
       "type": "airtable",  // Must be exactly one of the supported types
       "config": { ... }
     }
   }
   ```

### Backend Connection Issues

#### Issue: AirTable "Invalid API key"
```
Error: Backend connection failed: Invalid API key
```

**Solution:**
1. Verify your AirTable API key:
   - Go to https://airtable.com/account
   - Generate a new API key if needed
   - Ensure the key has access to your base
2. Check the API key format (should start with "key"):
   ```bash
   echo $AIRTABLE_API_KEY  # Should show: keyXXXXXXXXXXXXXX
   ```

#### Issue: AirTable "Base not found"
```
Error: Backend connection failed: Base not found
```

**Solution:**
1. Verify your AirTable base ID:
   - Go to https://airtable.com/api
   - Select your base
   - Copy the base ID from the URL (starts with "app")
2. Ensure your API key has access to the base:
   ```bash
   echo $AIRTABLE_BASE_ID  # Should show: appXXXXXXXXXXXXXX
   ```

#### Issue: Baserow connection timeout
```
Error: Backend connection failed: Connection timeout
```

**Solution:**
1. Check your Baserow base URL:
   ```json
   {
     "backend": {
       "type": "baserow",
       "config": {
         "base_url": "https://api.baserow.io",  // Correct format
         "api_key": "${BASEROW_API_KEY}",
         "database_id": "${BASEROW_DATABASE_ID}"
       }
     }
   }
   ```
2. Test connectivity manually:
   ```bash
   curl -H "Authorization: Token $BASEROW_API_KEY" $BASEROW_BASE_URL/api/database/
   ```

#### Issue: NocoDB authentication failed
```
Error: Backend connection failed: Authentication failed
```

**Solution:**
1. Verify your NocoDB API token:
   - Go to your NocoDB instance
   - Navigate to Account Settings → Tokens
   - Generate a new token if needed
2. Check the base URL format:
   ```json
   {
     "backend": {
       "type": "nocodb",
       "config": {
         "base_url": "https://your-nocodb-instance.com",  // No trailing slash
         "api_key": "${NOCODB_API_KEY}",
         "project_id": "${NOCODB_PROJECT_ID}"
       }
     }
   }
   ```

### Database Connection Issues

#### Issue: "Database connection failed"
```
Error: Database connection failed: could not connect to server
```

**Solution:**
1. Check if PostgreSQL is running:
   ```bash
   pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT
   ```
2. Verify connection parameters:
   ```bash
   psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB
   ```
3. Check firewall settings and network connectivity
4. Ensure the database exists:
   ```sql
   CREATE DATABASE your_database_name;
   ```

#### Issue: "Authentication failed for user"
```
Error: Database connection failed: FATAL: password authentication failed for user "username"
```

**Solution:**
1. Verify PostgreSQL credentials:
   ```bash
   echo $POSTGRES_USER
   echo $POSTGRES_PASSWORD  # Be careful with this in production
   ```
2. Check PostgreSQL authentication configuration (`pg_hba.conf`)
3. Ensure the user exists and has proper permissions:
   ```sql
   CREATE USER your_username WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE your_database TO your_username;
   ```

#### Issue: "Database does not exist"
```
Error: Database connection failed: FATAL: database "database_name" does not exist
```

**Solution:**
1. Create the database:
   ```sql
   CREATE DATABASE your_database_name;
   ```
2. Or connect to an existing database and update your configuration

### Sync Operation Issues

#### Issue: "Table not found in backend"
```
Error: Failed to retrieve data for elements: Table 'Elements' not found
```

**Solution:**
1. Check table names in your backend (case-sensitive)
2. Update table mappings in configuration:
   ```json
   {
     "table_mappings": {
       "elements": "Actual_Table_Name_In_Backend",
       "stations": "Another_Table_Name"
     }
   }
   ```
3. List available tables in your backend to verify names

#### Issue: "Schema mismatch" or "Column not found"
```
Error: Failed to sync table elements: column "field_name" does not exist
```

**Solution:**
1. Ensure your PostgreSQL schema matches the expected inventory schema
2. Use `mbx-db` to create or update the schema:
   ```bash
   mbx-db create-schema --schema inventory
   ```
3. Check for schema migrations:
   ```bash
   mbx-db migrate --schema inventory
   ```

#### Issue: "Constraint violation"
```
Error: Failed to sync table elements: duplicate key value violates unique constraint
```

**Solution:**
1. This usually indicates duplicate data in your backend
2. Check for duplicate records in your backend tables
3. Use dry-run mode to identify problematic records:
   ```bash
   mbx-inventory sync --dry-run --verbose --tables elements
   ```

#### Issue: "Sync operation timeout"
```
Error: Sync operation failed: Operation timed out after 30 seconds
```

**Solution:**
1. Increase timeout in configuration:
   ```json
   {
     "sync_options": {
       "timeout": 120,  // Increase from default 30 seconds
       "batch_size": 50,  // Reduce batch size
       "retry_attempts": 3
     }
   }
   ```
2. Sync tables individually:
   ```bash
   mbx-inventory sync --tables elements
   mbx-inventory sync --tables stations
   ```

### Performance Issues

#### Issue: Slow sync operations
**Solution:**
1. Adjust batch size in configuration:
   ```json
   {
     "sync_options": {
       "batch_size": 50,  // Reduce from default 100
       "timeout": 60,
       "retry_attempts": 3
     }
   }
   ```
2. Sync tables individually instead of all at once
3. Use verbose mode to identify bottlenecks:
   ```bash
   mbx-inventory sync --verbose
   ```

#### Issue: High memory usage
**Solution:**
1. Reduce batch size significantly:
   ```json
   {
     "sync_options": {
       "batch_size": 25,  // Very small batches
       "timeout": 120,
       "retry_attempts": 3
     }
   }
   ```
2. Sync one table at a time:
   ```bash
   for table in elements stations inventory; do
     mbx-inventory sync --tables $table
   done
   ```

## Advanced Troubleshooting

### Enable Debug Logging

For detailed debugging information, set the log level:

```bash
export PYTHONPATH=/path/to/mbx-inventory/src
export LOG_LEVEL=DEBUG
mbx-inventory sync --verbose
```

### Check Dependencies

Ensure all required packages are installed:

```bash
pip list | grep -E "(typer|rich|sqlalchemy|asyncpg|httpx|pydantic)"
```

### Test Individual Components

1. **Test configuration loading**:
   ```python
   from mbx_inventory.cli.config import InventoryConfig
   config = InventoryConfig.load_from_file("inventory_config.json")
   print(config)
   ```

2. **Test backend connectivity**:
   ```python
   backend = config.get_backend_instance()
   result = backend.validate()
   print(f"Backend validation: {result}")
   ```

3. **Test database connectivity**:
   ```python
   import asyncio
   result = asyncio.run(config.validate_database_connectivity())
   print(f"Database validation: {result}")
   ```

### Common Environment Issues

#### Issue: Command not found
```
bash: mbx-inventory: command not found
```

**Solution:**
1. Ensure the package is installed:
   ```bash
   pip install mbx-inventory
   ```
2. Check if the script is in your PATH:
   ```bash
   which mbx-inventory
   ```
3. If using a virtual environment, ensure it's activated:
   ```bash
   source venv/bin/activate  # or your virtual environment activation command
   ```

#### Issue: Import errors
```
ModuleNotFoundError: No module named 'mbx_inventory'
```

**Solution:**
1. Install in development mode if working with source:
   ```bash
   pip install -e .
   ```
2. Check Python path:
   ```bash
   python -c "import sys; print(sys.path)"
   ```

## Getting Additional Help

### Collecting Debug Information

When reporting issues, include:

1. **Version information**:
   ```bash
   mbx-inventory --version  # If available
   pip show mbx-inventory
   ```

2. **Configuration (sanitized)**:
   ```bash
   mbx-inventory config show  # Remove sensitive information before sharing
   ```

3. **Full error output with verbose logging**:
   ```bash
   mbx-inventory sync --verbose 2>&1 | tee debug.log
   ```

4. **Environment information**:
   ```bash
   python --version
   pip list
   echo $SHELL
   uname -a
   ```

### Support Resources

1. **Documentation**: Check the main README and documentation files
2. **Examples**: Review configuration examples in the `examples/` directory
3. **Tests**: Look at test files for usage patterns
4. **Source Code**: The CLI source is in `src/mbx_inventory/cli/`

### Creating Minimal Reproduction Cases

When troubleshooting complex issues:

1. **Create a minimal configuration**:
   ```json
   {
     "backend": {
       "type": "airtable",
       "config": {
         "api_key": "test_key",
         "base_id": "test_base"
       }
     },
     "database": {
       "host": "localhost",
       "port": 5432,
       "database": "test_db",
       "username": "test_user",
       "password": "test_pass"
     }
   }
   ```

2. **Test with dry-run mode**:
   ```bash
   mbx-inventory sync --config minimal_config.json --dry-run --verbose
   ```

3. **Isolate the issue** by testing individual components (config, backend, database) separately.

This systematic approach will help identify the root cause of most issues with the mbx-inventory CLI.