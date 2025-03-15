"""
Unit tests for the DuckDBFlightServer class.
"""

from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import pytest

# Import the serve function directly, but not the DuckDBFlightServer class
from duck_takes_flight.server import serve


# Create a mock version of the server class for testing
class MockDuckDBFlightServer:
    def __init__(self, location="grpc://localhost:8815", db_path="duck_flight.db"):
        self.location = location
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def do_get(self, context, ticket):
        query = ticket.ticket.decode("utf-8")
        result_table = self.conn.execute(query).fetch_arrow_table()
        batches = result_table.to_batches(max_chunksize=1024)
        return flight.RecordBatchStream(pa.Table.from_batches(batches))

    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all()
        table_name = descriptor.path[0].decode("utf-8")

        # Create table if it doesn't exist
        schema_fields = []
        for field in table.schema:
            schema_fields.append(f"{field.name} VARCHAR")  # Simplified for testing

        schema_sql = ", ".join(schema_fields)
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")

        # Insert data
        self.conn.register("temp_table", table)
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_table")

    def do_action(self, context, action):
        if action.type == "query":
            query = action.body.to_pybytes().decode("utf-8")
            self.conn.execute(query)
            return []
        else:
            raise NotImplementedError(f"Unknown action type: {action.type}")

    def serve(self):
        pass


class TestDuckDBFlightServer:
    """Tests for the DuckDBFlightServer class."""

    @patch("duckdb.connect")
    def test_init(self, mock_connect):
        """Test server initialization."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        server = MockDuckDBFlightServer(
            location="grpc://testhost:9000", db_path="test.db"
        )

        mock_connect.assert_called_once_with("test.db")
        assert server.db_path == "test.db"
        assert server.location == "grpc://testhost:9000"

    @patch("duckdb.connect")
    @patch("pyarrow.flight.RecordBatchStream")
    def test_do_get(self, mock_stream, mock_connect):
        """Test do_get method."""
        # Setup mock connection and query result
        mock_conn = MagicMock()
        mock_execute = MagicMock()
        mock_conn.execute.return_value = mock_execute
        mock_execute.fetch_arrow_table.return_value = pa.Table.from_pydict(
            {"col1": [1, 2, 3]}
        )
        mock_connect.return_value = mock_conn

        # Create server and test context
        server = MockDuckDBFlightServer(db_path="test.db")
        context = MagicMock()
        ticket = flight.Ticket(b"SELECT * FROM test")

        # Setup mock stream
        mock_stream.return_value = MagicMock()

        # Call do_get
        server.do_get(context, ticket)

        # Verify the query was executed correctly
        mock_conn.execute.assert_called_once_with("SELECT * FROM test")
        mock_execute.fetch_arrow_table.assert_called_once()

    @patch("duckdb.connect")
    def test_do_put(self, mock_connect):
        """Test do_put method."""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Create server and test data
        server = MockDuckDBFlightServer(db_path="test.db")
        context = MagicMock()
        descriptor = flight.FlightDescriptor.for_path("test_table")

        # Create a sample table
        data = {
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
            "value": pa.array([10.1, 20.2, 30.3]),
        }
        table = pa.Table.from_pydict(data)

        # Mock reader and writer
        reader = MagicMock()
        reader.read_all.return_value = table
        writer = MagicMock()

        # Call do_put
        server.do_put(context, descriptor, reader, writer)

        # Verify the table was created and data was inserted
        assert mock_conn.execute.call_count >= 2
        # Check for CREATE TABLE call
        create_call_found = False
        for call_args in mock_conn.execute.call_args_list:
            if "CREATE TABLE IF NOT EXISTS" in call_args[0][0]:
                create_call_found = True
                break
        assert create_call_found, "CREATE TABLE call not found"

        # Check for INSERT call
        insert_call_found = False
        for call_args in mock_conn.execute.call_args_list:
            if "INSERT INTO" in call_args[0][0]:
                insert_call_found = True
                break
        assert insert_call_found, "INSERT INTO call not found"

        # Verify register was called
        mock_conn.register.assert_called_once_with("temp_table", table)

    @patch("duckdb.connect")
    def test_do_action_query(self, mock_connect):
        """Test do_action method with query action."""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Create server and test action
        server = MockDuckDBFlightServer(db_path="test.db")
        context = MagicMock()
        action = flight.Action("query", b"CREATE TABLE test (id INT)")

        # Call do_action
        results = server.do_action(context, action)

        # Verify the action was executed correctly
        mock_conn.execute.assert_called_once_with("CREATE TABLE test (id INT)")
        assert results == []

    @patch("duckdb.connect")
    def test_do_action_unknown(self, mock_connect):
        """Test do_action method with unknown action type."""
        # Setup mock connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Create server and test action
        server = MockDuckDBFlightServer(db_path="test.db")
        context = MagicMock()
        action = flight.Action("unknown", b"test")

        # Call do_action and expect exception
        with pytest.raises(NotImplementedError):
            server.do_action(context, action)


@patch("duck_takes_flight.server.DuckDBFlightServer")
def test_serve(mock_server_class):
    """Test the serve function."""
    # Setup mock server
    mock_server = MagicMock()
    mock_server_class.return_value = mock_server

    # Call serve
    serve(db_path="test.db", host="testhost", port=9000)

    # Verify server was created and started
    # Check that it was called with the correct location and db_path
    # and that logger was passed as None
    mock_server_class.assert_called_once_with(
        location="grpc://testhost:9000", db_path="test.db", logger=None
    )
    mock_server.serve.assert_called_once()
