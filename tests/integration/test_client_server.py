"""
Integration tests for the DuckDB Flight client and server.
"""

import pyarrow as pa


class TestClientServerIntegration:
    """Integration tests for client and server."""

    def test_connection(self, flight_client):
        """Test that the client can connect to the server."""
        # The flight_client fixture will raise an exception if connection fails
        assert flight_client is not None

    def test_upload_and_query(self, flight_client, sample_table):
        """Test uploading data and querying it."""
        # Upload data
        table_name = "test_upload"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query the data
        query = f"SELECT * FROM {table_name}"
        result = flight_client.execute_query(query)

        # Verify the result
        assert result is not None
        assert result.num_rows == sample_table.num_rows
        assert result.column_names == sample_table.column_names

    def test_query_with_filter(self, flight_client, sample_table):
        """Test querying with a filter."""
        # Upload data
        table_name = "test_filter"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query with filter
        query = f"SELECT * FROM {table_name} WHERE value > 30"
        result = flight_client.execute_query(query)

        # Verify the result
        assert result is not None
        assert result.num_rows > 0
        assert result.num_rows < sample_table.num_rows

    def test_query_with_aggregation(self, flight_client, sample_table):
        """Test querying with aggregation."""
        # Upload data
        table_name = "test_aggregation"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query with aggregation
        query = f"SELECT AVG(value) as avg_value FROM {table_name}"
        result = flight_client.execute_query(query)

        # Verify the result
        assert result is not None
        assert result.num_rows == 1
        assert "avg_value" in result.column_names

    def test_multiple_uploads(self, flight_client):
        """Test multiple uploads to the same table."""
        # Create two tables with different data
        table1 = pa.Table.from_pydict(
            {"id": pa.array([1, 2, 3]), "value": pa.array([10, 20, 30])}
        )

        table2 = pa.Table.from_pydict(
            {"id": pa.array([4, 5, 6]), "value": pa.array([40, 50, 60])}
        )

        # Upload both tables to the same table name
        table_name = "test_multiple"
        success1 = flight_client.upload_data(table_name, table1)
        success2 = flight_client.upload_data(table_name, table2)

        assert success1 is True
        assert success2 is True

        # Query and verify we have all rows
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = flight_client.execute_query(query)

        assert result is not None
        assert result.column("count")[0].as_py() == 6

    def test_execute_action(self, flight_client):
        """Test executing a custom action."""
        # Create a table using an action
        action_type = "query"
        action_body = "CREATE TABLE test_action (id INTEGER, name VARCHAR)"

        results = flight_client.execute_action(action_type, action_body)
        assert results == []

        # Verify the table was created by inserting and querying data
        insert_query = "INSERT INTO test_action VALUES (1, 'test')"
        flight_client.execute_action("query", insert_query)

        # Query the data
        result = flight_client.execute_query("SELECT * FROM test_action")

        assert result is not None
        assert result.num_rows == 1
        assert result.column("id")[0].as_py() == 1
        assert result.column("name")[0].as_py() == "test"

    def test_error_handling(self, flight_client):
        """Test error handling for invalid queries."""
        # Execute an invalid query
        result = flight_client.execute_query("SELECT * FROM nonexistent_table")

        # The client should handle the error and return None
        assert result is None

    def test_execute_query_to_polars(self, flight_client, sample_table):
        """Test querying with conversion to Polars DataFrame."""
        # Upload data
        table_name = "test_polars"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query the data with conversion to Polars
        query = f"SELECT * FROM {table_name}"
        result_df = flight_client.execute_query_to_polars(query)

        # Verify the result
        assert result_df is not None
        assert len(result_df) == sample_table.num_rows
        # Check that column names match
        for col_name in sample_table.column_names:
            assert col_name in result_df.columns

    def test_execute_query_to_polars_with_filter(self, flight_client, sample_table):
        """Test querying with filter and conversion to Polars DataFrame."""
        # Upload data
        table_name = "test_polars_filter"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query with filter and conversion to Polars
        query = f"SELECT * FROM {table_name} WHERE value > 30"
        result_df = flight_client.execute_query_to_polars(query)

        # Verify the result
        assert result_df is not None
        assert len(result_df) > 0
        assert len(result_df) < sample_table.num_rows

    def test_execute_query_to_polars_with_aggregation(
        self, flight_client, sample_table
    ):
        """Test querying with aggregation and conversion to Polars DataFrame."""
        # Upload data
        table_name = "test_polars_aggregation"
        success = flight_client.upload_data(table_name, sample_table)
        assert success is True

        # Query with aggregation and conversion to Polars
        query = f"SELECT AVG(value) as avg_value FROM {table_name}"
        result_df = flight_client.execute_query_to_polars(query)

        # Verify the result
        assert result_df is not None
        assert len(result_df) == 1
        assert "avg_value" in result_df.columns

    def test_execute_query_to_polars_error_handling(self, flight_client):
        """Test error handling for invalid queries with Polars conversion."""
        # Execute an invalid query with Polars conversion
        result_df = flight_client.execute_query_to_polars(
            "SELECT * FROM nonexistent_table"
        )

        # The client should handle the error and return an empty DataFrame
        assert result_df is not None
        assert len(result_df) == 0
