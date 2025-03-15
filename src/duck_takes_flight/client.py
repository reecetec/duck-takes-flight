"""
DuckDB Flight Client implementation.
"""

import time

import pyarrow.flight as flight
from pyarrow._flight import FlightUnavailableError


class DuckDBFlightClient:
    """
    A client for interacting with the DuckDB Flight server.
    """

    def __init__(self, host="localhost", port=8815, max_attempts=5):
        """
        Initialize the DuckDB Flight client.

        Args:
            host: The host to connect to.
            port: The port to connect to.
            max_attempts: The maximum number of connection attempts.
        """
        self.location = f"grpc://{host}:{port}"
        self.client = self.connect_with_retry(max_attempts)

    def connect_with_retry(self, max_attempts=5):
        """
        Connect to the Flight server with retry logic.

        Args:
            max_attempts: The maximum number of connection attempts.

        Returns:
            A Flight client.

        Raises:
            FlightUnavailableError: If the server is unavailable after max_attempts.
        """
        for attempt in range(max_attempts):
            try:
                client = flight.connect(self.location)
                print(f"Connected to Flight server at {self.location}")
                return client
            except FlightUnavailableError:
                if attempt < max_attempts - 1:
                    print(
                        f"Connection attempt {attempt + 1} failed, retrying in 1 second..."
                    )
                    time.sleep(1)
                else:
                    raise

    def execute_query(self, query):
        """
        Execute a query on the Flight server.

        Args:
            query: The SQL query to execute.

        Returns:
            A PyArrow Table containing the query results.
        """
        try:
            ticket = flight.Ticket(query.encode("utf-8"))
            reader = self.client.do_get(ticket)
            result = reader.read_all()
            return result
        except Exception as e:
            print(f"Query error: {str(e)}")
            return None

    def upload_data(self, table_name, data):
        """
        Upload data to the Flight server.

        Args:
            table_name: The name of the table to upload to.
            data: A PyArrow Table containing the data to upload.

        Returns:
            True if the upload was successful, False otherwise.
        """
        try:
            descriptor = flight.FlightDescriptor.for_path(table_name)
            writer, _ = self.client.do_put(descriptor, data.schema)
            writer.write_table(data)
            writer.close()
            return True
        except Exception as e:
            print(f"Error uploading data: {str(e)}")
            return False

    def execute_action(self, action_type, body=None):
        """
        Execute a custom action on the Flight server.

        Args:
            action_type: The type of action to execute.
            body: The body of the action.

        Returns:
            A list of results.
        """
        try:
            action = flight.Action(action_type, body.encode() if body else None)
            return list(self.client.do_action(action))
        except Exception as e:
            print(f"Action error: {str(e)}")
            return []

    def list_tables(self):
        """
        List all tables in the database.

        Returns:
            A list of table names.
        """
        results = self.execute_action("list_tables")
        return [result.body.to_pybytes().decode() for result in results]

    def create_table(self, table_name, schema_sql):
        """
        Create a table in the database.

        Args:
            table_name: The name of the table to create.
            schema_sql: The SQL schema for the table.

        Returns:
            True if the table was created successfully, False otherwise.
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})"
        return self.execute_action("query", query) is not None
