"""Main CLI application for mbx-inventory."""

import asyncio
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel

from .config import InventoryConfig
from .progress import ProgressReporter
from .exceptions import (
    InventoryCLIError,
)

# Create the main typer application
app = typer.Typer(
    help="Inventory synchronization CLI for Mesonet-in-a-Box\n\n"
    "This CLI tool synchronizes inventory data from various backends (AirTable, Baserow, NocoDB) "
    "to a PostgreSQL database. It provides commands for configuration management, validation, "
    "and data synchronization with support for dry-run previews and selective table syncing.\n\n"
    "Examples:\n"
    "  mbx-inventory validate --config inventory_config.json\n"
    "  mbx-inventory sync --dry-run --verbose\n"
    "  mbx-inventory sync --tables elements,stations\n"
    "  mbx-inventory config show",
    no_args_is_help=True,
)

# Create console for rich output
console = Console()


@app.command()
def validate(
    config_file: Path = typer.Option(
        Path("inventory_config.json"),
        "--config",
        "-c",
        help="Path to configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """Validate backend connection and configuration.

    This command validates your inventory configuration file and tests connectivity
    to both the backend (AirTable/Baserow/NocoDB) and PostgreSQL database.

    The validation process includes:
    - Configuration file syntax and structure validation
    - Environment variable availability check
    - Backend API connectivity test
    - PostgreSQL database connectivity test
    - Table mapping validation

    Examples:
      mbx-inventory validate
      mbx-inventory validate --config my_config.json --verbose
    """
    # Initialize progress reporter
    reporter = ProgressReporter(verbose=verbose, console=console)
    load_dotenv()
    try:
        with reporter.operation_context("Configuration Validation", total_items=5):
            # Load configuration
            reporter.log_info("Loading configuration file", {"file": str(config_file)})
            config = InventoryConfig.load_from_file(config_file)
            reporter.update_progress(1, "Configuration loaded successfully")

            reporter.log_info(f"Backend: {config.backend.type}")
            reporter.log_info(
                f"Database: {config.database.host}:{config.database.port}/{config.database.database}"
            )

            # Validate environment variables
            reporter.update_progress(1, "Validating environment variables...")
            if not config.validate_environment_variables():
                reporter.report_error("Some required environment variables are not set")
                raise typer.Exit(1)
            reporter.log_info("Environment variables validated")

            # Validate backend connectivity
            reporter.update_progress(1, "Testing backend connection...")
            try:
                backend = config.get_backend_instance()
                if backend.validate():
                    reporter.log_info("Backend connection successful")
                else:
                    reporter.report_error("Backend connection failed")
                    raise typer.Exit(1)
            except Exception as e:
                reporter.report_error("Backend connection error", {"error": str(e)})
                raise typer.Exit(1)

            # Test database connectivity
            reporter.update_progress(1, "Testing database connectivity...")
            try:
                if asyncio.run(config.validate_database_connectivity()):
                    reporter.log_info("Database connection successful")
                else:
                    reporter.report_error("Database connection failed")
                    raise typer.Exit(1)
            except Exception as e:
                reporter.report_error("Database connection error", {"error": str(e)})
                raise typer.Exit(1)

            # Validate table mappings
            reporter.update_progress(1, "Validating table mappings...")
            if config.table_mappings:
                if config.validate_table_mappings():
                    reporter.log_info("Table mappings validated")
                else:
                    reporter.report_error("Table mappings validation failed")
                    raise typer.Exit(1)
            else:
                reporter.log_info("No table mappings to validate")

            reporter.complete_operation("All validation checks passed")

    except InventoryCLIError as e:
        # Handle our custom errors with detailed formatting
        reporter.report_error(e.get_formatted_message())
        raise typer.Exit(1)
    except Exception as e:
        reporter.report_error(f"Unexpected error: {e}")
        raise typer.Exit(1)


@app.command()
def sync(
    config_file: Path = typer.Option(
        Path("inventory_config.json"),
        "--config",
        "-c",
        help="Path to configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview changes without executing them"
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        help="Comma-separated list of tables to sync (sync all if not specified)",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """Sync inventory data from backend to PostgreSQL.

    This command synchronizes inventory data from your configured backend
    (AirTable, Baserow, or NocoDB) to a PostgreSQL database. The sync process
    handles both creating new records and updating existing ones.

    Features:
    - Dry-run mode to preview changes without applying them
    - Selective table synchronization with --tables option
    - Progress reporting and detailed logging
    - Automatic retry logic for failed operations
    - Transaction rollback on errors

    Available tables: elements, stations, component_models, inventory,
    deployments, component_elements, request_schemas, response_schemas

    Examples:
      mbx-inventory sync --dry-run
      mbx-inventory sync --tables elements,stations --verbose
      mbx-inventory sync --config production_config.json
    """
    load_dotenv()

    # Initialize progress reporter
    reporter = ProgressReporter(verbose=verbose, console=console)

    async def run_sync():
        try:
            reporter.log_info("Starting inventory synchronization")

            # Load configuration
            reporter.log_info("Loading configuration file", {"file": str(config_file)})
            config = InventoryConfig.load_from_file(config_file)

            # Parse tables list if provided
            table_list = None
            if tables:
                table_list = [t.strip() for t in tables.split(",")]
                reporter.log_info("Tables to sync", {"tables": table_list})
            else:
                reporter.log_info("Syncing all configured tables")

            # Log operation mode
            if dry_run:
                reporter.log_info("Running in dry-run mode - no changes will be made")
            else:
                reporter.log_info("Running in live mode - changes will be applied")

            # Create database engine
            from mbx_db import make_connection_string
            from sqlalchemy.ext.asyncio import create_async_engine

            connection_string = make_connection_string(
                username=config.database.username,
                password=config.database.password,
                host=config.database.host,
                database=config.database.database,
                port=config.database.port,
            )

            engine = create_async_engine(connection_string)

            try:
                # Get inventory instance
                inventory = config.get_inventory_instance()

                # Create sync engine
                from .sync_engine import SyncEngine

                sync_engine = SyncEngine(
                    inventory=inventory,
                    db_engine=engine,
                    progress_reporter=reporter,
                    batch_size=config.sync_options.batch_size,
                )

                # Run synchronization
                sync_result = await sync_engine.sync_all_tables(
                    dry_run=dry_run,
                    table_filter=table_list,
                )

                # Display summary
                if dry_run:
                    reporter.log_info("Dry run completed successfully")
                else:
                    reporter.log_info("Synchronization completed successfully")

                # Display detailed results
                summary_data = []
                for table_result in sync_result.table_results:
                    summary_data.append(
                        {
                            "table": table_result.table_name.split(".")[
                                -1
                            ],  # Remove schema prefix
                            "created": table_result.records_created,
                            "updated": table_result.records_updated,
                            "failed": table_result.records_failed,
                            "duration": f"{table_result.duration_seconds:.2f}s",
                        }
                    )

                if summary_data:
                    reporter.display_summary_table("Sync Results", summary_data)

                # Display overall statistics
                total_duration = sync_result.total_duration_seconds
                reporter.log_info(f"Total operation time: {total_duration:.2f} seconds")
                reporter.log_info(
                    f"Tables processed: {sync_result.successful_tables}/{sync_result.total_tables}"
                )
                reporter.log_info(
                    f"Records created: {sync_result.total_records_created}"
                )
                reporter.log_info(
                    f"Records updated: {sync_result.total_records_updated}"
                )

                if sync_result.errors:
                    reporter.report_warning(
                        f"Encountered {len(sync_result.errors)} errors during sync"
                    )
                    for error in sync_result.errors:
                        reporter.log_debug(f"Error: {error}")

            finally:
                await engine.dispose()

        except InventoryCLIError as e:
            # Handle our custom errors with detailed formatting
            reporter.report_error(e.get_formatted_message())
            raise typer.Exit(1)
        except Exception as e:
            reporter.report_error(f"Sync operation failed: {e}")
            raise typer.Exit(1)

    # Run the async sync operation
    asyncio.run(run_sync())


# Config subcommand group
config_app = typer.Typer(
    help="Configuration management commands\n\n"
    "These commands help you manage and validate your inventory configuration files. "
    "Configuration files define backend connections, database settings, table mappings, "
    "and synchronization options."
)
app.add_typer(config_app, name="config")


@config_app.command("show")
def config_show(
    config_file: Path = typer.Option(
        Path("inventory_config.json"),
        "--config",
        "-c",
        help="Path to configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
):
    """Display current configuration in a formatted view.

    This command loads and displays your inventory configuration file
    in a human-readable format, showing backend settings, database
    configuration, table mappings, and sync options.

    Examples:
      mbx-inventory config show
      mbx-inventory config show --config production_config.json
    """
    try:
        config = InventoryConfig.load_from_file(config_file)

        # Create a formatted display of the configuration
        config_display = f"""
[bold blue]Backend Configuration:[/bold blue]
  Type: {config.backend.type}
  Config keys: {", ".join(config.backend.config.keys())}

[bold blue]Database Configuration:[/bold blue]
  Host: {config.database.host}
  Port: {config.database.port}
  Database: {config.database.database}
  Username: {config.database.username}

[bold blue]Sync Options:[/bold blue]
  Batch size: {config.sync_options.batch_size}
  Timeout: {config.sync_options.timeout}s
  Retry attempts: {config.sync_options.retry_attempts}
"""

        if config.table_mappings:
            mappings = "\n  ".join(
                [f"{k}: {v}" for k, v in config.table_mappings.items()]
            )
            config_display += f"\n[bold blue]Table Mappings:[/bold blue]\n  {mappings}"

        console.print(Panel(config_display.strip(), title="Inventory Configuration"))

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


@config_app.command("validate")
def config_validate(
    config_file: Path = typer.Option(
        Path("inventory_config.json"),
        "--config",
        "-c",
        help="Path to configuration file",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    test_connectivity: bool = typer.Option(
        False, "--test-connectivity", help="Test backend and database connectivity"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
):
    """Validate configuration file structure and settings.

    This command validates your configuration file syntax, structure,
    and environment variable availability. Use --test-connectivity to
    also test actual connections to backend and database services.

    Validation includes:
    - JSON syntax validation
    - Required field presence
    - Backend type validation
    - Environment variable availability
    - Optional connectivity testing

    Examples:
      mbx-inventory config validate
      mbx-inventory config validate --test-connectivity --verbose
    """
    # Initialize progress reporter
    reporter = ProgressReporter(verbose=verbose, console=console)

    try:
        reporter.log_info(
            "Starting configuration validation", {"file": str(config_file)}
        )
        config = InventoryConfig.load_from_file(config_file)

        reporter.log_info("Configuration file is valid")
        reporter.log_info(f"Backend: {config.backend.type}")
        reporter.log_info(f"Database: {config.database.host}:{config.database.port}")

        # Validate environment variables
        reporter.log_debug("Checking environment variables...")
        if config.validate_environment_variables():
            reporter.log_info("All environment variables are set")
        else:
            reporter.report_warning("Some environment variables may not be set")

        # Test connectivity if requested
        if test_connectivity:
            reporter.log_info("Testing connectivity...")

            reporter.log_debug("Testing backend connectivity...")
            try:
                backend = config.get_backend_instance()
                if backend.validate():
                    reporter.log_info("Backend connection successful")
                else:
                    reporter.report_error("Backend connection failed")
            except Exception as e:
                reporter.report_error("Backend connection error", {"error": str(e)})

            reporter.log_debug("Testing database connectivity...")
            try:
                if asyncio.run(config.validate_database_connectivity()):
                    reporter.log_info("Database connection successful")
                else:
                    reporter.report_error("Database connection failed")
            except Exception as e:
                reporter.report_error("Database connection error", {"error": str(e)})

        reporter.log_info("Configuration validation complete")

    except InventoryCLIError as e:
        # Handle our custom errors with detailed formatting
        reporter.report_error(e.get_formatted_message())
        raise typer.Exit(1)
    except Exception as e:
        reporter.report_error(f"Unexpected error: {e}")
        raise typer.Exit(1)
