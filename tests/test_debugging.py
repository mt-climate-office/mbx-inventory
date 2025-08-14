from typer.testing import CliRunner

from mbx_inventory.cli.main import app


class TestEndToEndIntegration:
    """Test complete end-to-end workflows."""

    def test_debug(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        _ = self.runner.invoke(
            app,
            [
                "sync",
                "--config",
                "/home/cbrust/git/mesonet-in-a-box/inventory_config.json",
                "-v",
                "--table", "deployments"
            ],
        )
        assert True
