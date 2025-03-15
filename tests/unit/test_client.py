"""
Unit tests for the DuckDBFlightClient class.
"""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyarrow._flight import FlightUnavailableError

from duck_takes_flight.client import DuckDBFlightClient


@pytest.fixture
def mock_flight_client():
    """Fixture to create a mocked Flight client."""
    with patch("pyarrow.flight.connect") as mock_connect:
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        yield mock_client


class TestDuckDBFlightClient:
    """Tests for the DuckDBFlightClient class."""

    def test_init_with_default_params(self):
        """Test client initialization with default parameters."""
        with patch("pyarrow.flight.connect") as mock_connect:
            mock_client = MagicMock()
            mock_connect.return_value = mock_client

            client = DuckDBFlightClient()

            assert client.location == "grpc://localhost:8815"
            mock_connect.assert_called_once_with("grpc://localhost:8815")

    def test_init_with_custom_params(self):
        """Test client initialization with custom parameters."""
        with patch("pyarrow.flight.connect") as mock_connect:
            mock_client = MagicMock()
            mock_connect.return_value = mock_client

            client = DuckDBFlightClient(host="testhost", port=9000)

            assert client.location == "grpc://testhost:9000"
            mock_connect.assert_called_once_with("grpc://testhost:9000")

    def test_connect_with_retry_success_first_attempt(self, mock_flight_client):
        """Test successful connection on first attempt."""
        with patch("pyarrow.flight.connect") as mock_connect:
            mock_connect.return_value = mock_flight_client

            client = DuckDBFlightClient()

            assert mock_connect.call_count == 1
            assert client.client == mock_flight_client

    def test_connect_with_retry_success_after_failures(self):
        """Test successful connection after failures."""
        with (
            patch("pyarrow.flight.connect") as mock_connect,
            patch("time.sleep") as mock_sleep,
        ):
            # Make connect fail twice then succeed
            mock_connect.side_effect = [
                FlightUnavailableError("Server unavailable"),
                FlightUnavailableError("Server unavailable"),
                MagicMock(),
            ]

            client = DuckDBFlightClient()

            assert mock_connect.call_count == 3
            assert mock_sleep.call_count == 2
            assert client.client is not None

    def test_connect_with_retry_all_failures(self):
        """Test connection failure after max attempts."""
        with (
            patch("pyarrow.flight.connect") as mock_connect,
            patch("time.sleep") as mock_sleep,
        ):
            # Make connect fail all times
            mock_connect.side_effect = FlightUnavailableError("Server unavailable")

            with pytest.raises(FlightUnavailableError):
                DuckDBFlightClient(max_attempts=3)

            assert mock_connect.call_count == 3
            assert mock_sleep.call_count == 2

    def test_execute_query(self, mock_flight_client):
        """Test query execution."""
        # Setup mock response
        mock_reader = MagicMock()
        mock_result = pa.Table.from_pydict({"col1": [1, 2, 3]})
        mock_reader.read_all.return_value = mock_result
        mock_flight_client.do_get.return_value = mock_reader

        client = DuckDBFlightClient()
        result = client.execute_query("SELECT * FROM test")

        # Verify the query was executed correctly
        mock_flight_client.do_get.assert_called_once()
        ticket_arg = mock_flight_client.do_get.call_args[0][0]
        assert ticket_arg.ticket == b"SELECT * FROM test"
        assert result == mock_result

    def test_execute_query_error(self, mock_flight_client):
        """Test query execution with error."""
        # Setup mock to raise exception
        mock_flight_client.do_get.side_effect = Exception("Query error")

        client = DuckDBFlightClient()
        result = client.execute_query("SELECT * FROM test")

        # Verify error handling
        assert result is None

    def test_upload_data(self, mock_flight_client, sample_table):
        """Test data upload."""
        # Setup mock writer
        mock_writer = MagicMock()
        mock_flight_client.do_put.return_value = (mock_writer, None)

        client = DuckDBFlightClient()
        result = client.upload_data("test_table", sample_table)

        # Verify the upload was executed correctly
        mock_flight_client.do_put.assert_called_once()
        descriptor_arg = mock_flight_client.do_put.call_args[0][0]
        # Check that the path is a list with the table name as bytes
        assert len(descriptor_arg.path) == 1
        assert descriptor_arg.path[0] == b"test_table"
        mock_writer.write_table.assert_called_once_with(sample_table)
        mock_writer.close.assert_called_once()
        assert result is True

    def test_upload_data_error(self, mock_flight_client, sample_table):
        """Test data upload with error."""
        # Setup mock to raise exception
        mock_flight_client.do_put.side_effect = Exception("Upload error")

        client = DuckDBFlightClient()
        result = client.upload_data("test_table", sample_table)

        # Verify error handling
        assert result is False

    def test_execute_action(self, mock_flight_client):
        """Test action execution."""
        # Setup mock response
        mock_results = [b"result1", b"result2"]
        mock_flight_client.do_action.return_value = mock_results

        client = DuckDBFlightClient()
        results = client.execute_action("query", "CREATE TABLE test")

        # Verify the action was executed correctly
        mock_flight_client.do_action.assert_called_once()
        action_arg = mock_flight_client.do_action.call_args[0][0]
        assert action_arg.type == "query"
        assert action_arg.body == b"CREATE TABLE test"
        assert results == mock_results

    @patch("duck_takes_flight.client.flight.Action")
    def test_execute_action_no_body(self, mock_action):
        """Test action execution with no body."""
        # Create a mock action
        mock_action_instance = MagicMock()
        mock_action.return_value = mock_action_instance

        # Create a mock client
        with patch("pyarrow.flight.connect") as mock_connect:
            mock_client = MagicMock()
            mock_connect.return_value = mock_client

            # Setup mock response
            mock_results = [b"result1", b"result2"]
            mock_client.do_action.return_value = mock_results

            # Create client and call method
            client = DuckDBFlightClient()
            results = client.execute_action("list_tables")

            # Verify the action was created correctly
            mock_action.assert_called_once_with("list_tables", None)

            # Verify do_action was called with our mock action
            mock_client.do_action.assert_called_once_with(mock_action_instance)

            # Verify results
            assert results == mock_results

    def test_execute_action_error(self, mock_flight_client):
        """Test action execution with error."""
        # Setup mock to raise exception
        mock_flight_client.do_action.side_effect = Exception("Action error")

        client = DuckDBFlightClient()
        results = client.execute_action("query", "CREATE TABLE test")

        # Verify error handling
        assert results == []
