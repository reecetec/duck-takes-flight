"""
DuckDB Flight Client implementation.
"""

import logging
import time
from typing import Optional

import polars as pl
import pyarrow.flight as flight
from pyarrow._flight import FlightUnavailableError

from .logging import logger as default_logger


class DuckDBFlightClient:
    """
    A client for interacting with the DuckDB Flight server.
    """

    def __init__(
        self,
        host="localhost",
        port=8815,
        max_attempts=5,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the DuckDB Flight client.

        Args:
            host: The host to connect to.
            port: The port to connect to.
            max_attempts: The maximum number of connection attempts.
            logger: Optional logger instance.
        """
        self.location = f"grpc://{host}:{port}"
        self.logger = logger or default_logger
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
                self.logger.info(f"Connected to Flight server at {self.location}")
                return client
            except FlightUnavailableError:
                if attempt < max_attempts - 1:
                    self.logger.warning(
                        f"Connection attempt {attempt + 1} failed, retrying in 1 second..."
                    )
                    time.sleep(1)
                else:
                    self.logger.error(
                        f"Failed to connect to {self.location} after {max_attempts} attempts"
                    )
                    raise

    def execute_query(self, query):
        """
        Execute a query on the Flight server.

        Args:
            query: The SQL query to execute.

        Returns:
            A PyArrow Table containing the query results, or an empty table
            if the query returns no rows.
        """
        try:
            self.logger.debug(f"Executing query: {query}")
            ticket = flight.Ticket(query.encode("utf-8"))
            reader = self.client.do_get(ticket)
            try:
                result = reader.read_all()
                self.logger.debug(f"Query returned {result.num_rows} rows")
                return result
            except ValueError as ve:
                if "Must pass schema, or at least one RecordBatch" in str(ve):
                    self.logger.debug("Query returned no rows, creating empty table")
                    # Create an empty table with appropriate schema if possible
                    # If we can't determine the schema, return an empty table with no columns
                    import pyarrow as pa

                    return pa.Table.from_arrays([], [])
                else:
                    # Re-raise if it's a different ValueError
                    raise
        except Exception as e:
            self.logger.error(f"Query error: {str(e)}")
            return None

    def execute_query_to_polars(self, query):
        """
        Execute a query on the Flight server and return the result as a Polars DataFrame.

        Args:
            query: The SQL query to execute.

        Returns:
            A Polars DataFrame containing the query results, or an empty DataFrame
            if the query returns no rows or an error occurs.
        """
        try:
            self.logger.debug(f"Executing query for Polars conversion: {query}")
            arrow_table = self.execute_query(query)

            if arrow_table is None:
                self.logger.warning(
                    "Query returned None, returning empty Polars DataFrame"
                )

                return pl.DataFrame()

            df = pl.from_arrow(arrow_table)
            self.logger.debug(
                f"Converted PyArrow table to Polars DataFrame with {len(df)} rows"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error converting to Polars DataFrame: {str(e)}")
            return pl.DataFrame()

    def upload_data(self, table_name, table):
        """
        Upload data to the Flight server.

        Args:
            table_name: The name of the table to upload to.
            table: A PyArrow Table containing the data to upload.

        Returns:
            True if the upload was successful, False otherwise.
        """
        try:
            self.logger.info(f"Uploading {table.num_rows} rows to table {table_name}")
            descriptor = flight.FlightDescriptor.for_path(table_name)
            writer, _ = self.client.do_put(descriptor, table.schema)
            writer.write_table(table)
            writer.close()
            self.logger.info(f"Successfully uploaded data to {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading data: {str(e)}")
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
            self.logger.debug(f"Executing action: {action_type}")
            action = flight.Action(action_type, body.encode() if body else None)
            results = list(self.client.do_action(action))
            self.logger.debug(f"Action completed successfully")
            return results
        except Exception as e:
            self.logger.error(f"Action error: {str(e)}")
            return []
