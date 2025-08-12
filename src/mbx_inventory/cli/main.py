"""Main CLI application for mbx-inventory."""

import asyncio
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel

from .config import (
    InventoryConfig,
    ConfigurationError,
)
from .progress import ProgressReporter

# Create the main typer application
app = typer.Typer(
    help="Inventory synchronization CLI for Mesonet-in-a-Box",
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
    """Validate backend connection and configuration."""
    # Initialize progress reporter
    reporter = ProgressReporter(verbose=verbose, console=console)

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

    except ConfigurationError as e:
        reporter.report_error(f"Configuration Error: {e}")
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
    load_dotenv()
    """Sync inventory data from backend to PostgreSQL."""
    # Initialize progress reporter
    reporter = ProgressReporter(verbose=verbose, console=console)

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

        # Placeholder for sync implementation
        reporter.report_warning(
            "Sync functionality will be implemented in subsequent tasks"
        )

        reporter.log_info("Sync operation completed")

    except ConfigurationError as e:
        reporter.report_error(f"Configuration Error: {e}")
        raise typer.Exit(1)
    except Exception as e:
        reporter.report_error(f"Sync operation failed: {e}")
        raise typer.Exit(1)


# Config subcommand group
config_app = typer.Typer(help="Configuration management commands")
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
    """Display current configuration."""
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
    """Validate configuration file."""
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

    except ConfigurationError as e:
        reporter.report_error(f"Configuration Error: {e}")
        raise typer.Exit(1)
    except Exception as e:
        reporter.report_error(f"Unexpected error: {e}")
        raise typer.Exit(1)
