"""
Unit tests for the CLI module.
"""

import unittest
from unittest.mock import MagicMock, patch

from duck_takes_flight.cli import main


class TestCLI(unittest.TestCase):
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
        # Add log level and log file attributes
        mock_args.log_level = "INFO"
        mock_args.log_file = None
        mock_parse_args.return_value = mock_args

        # Call main
        main()

        # Verify serve was called with correct arguments
        # The last argument is the logger, which we can't directly compare
        # So we check that it was called with the first 3 arguments
        self.assertEqual(
            mock_serve.call_args[0][:3], ("duck_flight.db", "localhost", 8815)
        )
        # And verify that a logger was passed as the 4th argument
        self.assertIsNotNone(mock_serve.call_args[0][3])

    @patch("duck_takes_flight.cli.serve")
    @patch("argparse.ArgumentParser.parse_args")
    def test_main_custom_args(self, mock_parse_args, mock_serve):
        """Test main function with custom arguments."""
        # Setup mock args
        mock_args = MagicMock()
        mock_args.db_path = "custom.db"
        mock_args.host = "127.0.0.1"
        mock_args.port = 9000
        # Add log level and log file attributes
        mock_args.log_level = "DEBUG"
        mock_args.log_file = "test.log"
        mock_parse_args.return_value = mock_args

        # Call main
        main()

        # Verify serve was called with correct arguments
        # The last argument is the logger, which we can't directly compare
        # So we check that it was called with the first 3 arguments
        self.assertEqual(mock_serve.call_args[0][:3], ("custom.db", "127.0.0.1", 9000))
        # And verify that a logger was passed as the 4th argument
        self.assertIsNotNone(mock_serve.call_args[0][3])
