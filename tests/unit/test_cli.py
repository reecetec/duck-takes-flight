"""
Unit tests for the CLI module.
"""

from unittest.mock import MagicMock, patch

from duck_takes_flight.cli import main


class TestCLI:
    """Tests for the CLI module."""

    @patch("duck_takes_flight.cli.serve")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_default_args(self, mock_parse_args, mock_serve):
        """Test main function with default arguments."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.db_path = "duck_flight.db"
        mock_args.host = "localhost"
        mock_args.port = 8815
        mock_parse_args.return_value = mock_args

        # Call main
        main()

        # Verify serve was called with correct arguments
        mock_serve.assert_called_once_with("duck_flight.db", "localhost", 8815)

    @patch("duck_takes_flight.cli.serve")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_custom_args(self, mock_parse_args, mock_serve):
        """Test main function with custom arguments."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.db_path = "custom.db"
        mock_args.host = "127.0.0.1"
        mock_args.port = 9000
        mock_parse_args.return_value = mock_args

        # Call main
        main()

        # Verify serve was called with correct arguments
        mock_serve.assert_called_once_with("custom.db", "127.0.0.1", 9000)
