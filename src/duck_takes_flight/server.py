"""
DuckDB Flight Server implementation.
"""

import logging
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.flight as flight

from .logging import logger as default_logger


class DuckDBFlightServer(flight.FlightServerBase):
    """
    A Flight server that exposes DuckDB through Arrow Flight protocol.
    """

    def __init__(
        self,
        location="grpc://localhost:8815",
        db_path="duck_flight.db",
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the DuckDB Flight server.

        Args:
            location: The location to bind the server to.
            db_path: The path to the DuckDB database file.
            logger: Optional logger instance.
        """
        super().__init__(location)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.logger = logger or default_logger
        self.logger.info(f"Connected to DuckDB database at {db_path}")

    def do_get(self, context, ticket):
        """
        Handle 'GET' requests from clients to retrieve data.

        Args:
            context: The call context.
            ticket: The ticket containing the query.

        Returns:
            A RecordBatchStream containing the query results.
        """
        query = ticket.ticket.decode("utf-8")
        self.logger.debug(f"Executing query: {query}")
        result_table = self.conn.execute(query).fetch_arrow_table()
        # Convert to record batches with alignment
        batches = result_table.to_batches(
            max_chunksize=1024
        )  # Use power of 2 for alignment
        self.logger.debug(f"Query returned {result_table.num_rows} rows")
        return flight.RecordBatchStream(pa.Table.from_batches(batches))

    def do_put(self, context, descriptor, reader, writer):
        """
        Handle 'PUT' requests to upload data to the DuckDB instance.

        Args:
            context: The call context.
            descriptor: The descriptor containing the table name.
            reader: The reader containing the data.
            writer: The writer to write results to.
        """
        table = reader.read_all()
        table_name = descriptor.path[0].decode("utf-8")

        self.logger.info(
            f"Received PUT request for table {table_name} with {table.num_rows} rows"
        )

        # Infer schema from the table
        schema_fields = []
        for field in table.schema:
            field_type = field.type
            if pa.types.is_integer(field_type):
                duckdb_type = "BIGINT"
            elif pa.types.is_floating(field_type):
                duckdb_type = "DOUBLE"
            elif pa.types.is_string(field_type):
                duckdb_type = "VARCHAR"
            elif pa.types.is_boolean(field_type):
                duckdb_type = "BOOLEAN"
            elif pa.types.is_timestamp(field_type):
                duckdb_type = "TIMESTAMP"
            elif pa.types.is_date(field_type):
                duckdb_type = "DATE"
            else:
                duckdb_type = "VARCHAR"  # Default to VARCHAR for unknown types

            schema_fields.append(f"{field.name} {duckdb_type}")

        # Create table if it doesn't exist
        schema_sql = ", ".join(schema_fields)
        self.logger.debug(f"Creating table with schema: {schema_sql}")
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql})")

        # Convert to record batches for better alignment
        batches = table.to_batches(max_chunksize=1024)
        aligned_table = pa.Table.from_batches(batches)
        self.conn.register("temp_table", aligned_table)

        # Insert new data
        self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_table")
        self.logger.info(f"Inserted {table.num_rows} rows into table {table_name}")

    def do_action(self, context, action):
        """
        Handle custom actions like executing SQL queries.

        Args:
            context: The call context.
            action: The action to perform.

        Returns:
            A list of results.
        """
        if action.type == "query":
            query = action.body.to_pybytes().decode("utf-8")
            self.logger.debug(f"Executing action query: {query}")
            self.conn.execute(query)
            return []
        else:
            error_msg = f"Unknown action type: {action.type}"
            self.logger.error(error_msg)
            raise NotImplementedError(error_msg)


def serve(
    db_path="duck_flight.db",
    host="localhost",
    port=8815,
    logger: Optional[logging.Logger] = None,
):
    """
    Start the DuckDB Flight server.

    Args:
        db_path: The path to the DuckDB database file.
        host: The host to bind to.
        port: The port to bind to.
        logger: Optional logger instance.
    """
    location = f"grpc://{host}:{port}"
    server = DuckDBFlightServer(location=location, db_path=db_path, logger=logger)
    logger = logger or default_logger
    logger.info(
        f"Starting DuckDB Flight server on {host}:{port} with database {db_path}"
    )
    server.serve()
