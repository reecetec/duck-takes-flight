import argparse
import random
import time
from datetime import datetime

import pyarrow as pa

from duck_takes_flight.client import DuckDBFlightClient


def generate_batch(batch_id, num_rows=1000):
    """Generate a test batch of data."""
    data = {
        "batch_id": [batch_id] * num_rows,
        "timestamp": [datetime.now().isoformat()] * num_rows,
        "value": [random.uniform(0, 100) for _ in range(num_rows)],
        "category": [random.choice(["A", "B", "C", "D"]) for _ in range(num_rows)],
    }
    return pa.Table.from_pydict(data)


def test_client(host="localhost", port=8815, num_batches=5, query_interval=1.0):
    """Test the DuckDBFlightClient with data loading and querying."""
    print(f"Testing DuckDBFlightClient connecting to {host}:{port}")

    # Initialize client
    client = DuckDBFlightClient(host=host, port=port)
    table_name = "test_table"

    # Upload data in batches
    for batch_id in range(num_batches):
        table = generate_batch(batch_id)
        print(f"Uploading batch {batch_id} with {table.num_rows} rows...")
        success = client.upload_data(table_name, table)
        if success:
            print(f"Successfully uploaded batch {batch_id}")
        else:
            print(f"Failed to upload batch {batch_id}")

        # Run a query after each batch upload
        print(f"Querying data after batch {batch_id}...")

        # Query 1: Count rows
        count_query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
        result = client.execute_query(count_query)
        if result is not None:
            print(f"Total rows: {result}")

        # Query 2: Group by category
        group_query = f"SELECT category, AVG(value) as avg_value FROM {table_name} GROUP BY category"
        result = client.execute_query(group_query)
        if result is not None:
            print(f"Category averages:\n{result}")

        # Query 3: Get latest batch
        latest_query = f"SELECT * FROM {table_name} WHERE batch_id = {batch_id} LIMIT 5"
        result = client.execute_query(latest_query)
        if result is not None:
            print(f"Sample from latest batch:\n{result}")

        # Wait between batches
        if batch_id < num_batches - 1:
            print(f"Waiting {query_interval} seconds before next batch...")
            time.sleep(query_interval)

    # Final test: Run a custom action
    print("\nTesting custom action...")
    action_results = client.execute_action(
        "query", f"SELECT count(1) FROM {table_name}"
    )
    print(f"Action results: {action_results}")
    # Final test: Run a custom action
    print("\nTesting custom action...")
    action_results = client.execute_action(
        "query",
        f"CREATE TABLE IF NOT EXISTS {table_name}2 (batch_id BIGINT, timestamp VARCHAR, value DOUBLE, category VARCHAR)",
    )
    print(f"Action results: {action_results}")

    print("\nClient test completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the DuckDBFlightClient")
    parser.add_argument(
        "--host", type=str, default="localhost", help="Host to connect to"
    )
    parser.add_argument("--port", type=int, default=8815, help="Port to connect to")
    parser.add_argument(
        "--batches", type=int, default=5, help="Number of batches to upload"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Interval between queries in seconds",
    )

    args = parser.parse_args()

    test_client(
        host=args.host,
        port=args.port,
        num_batches=args.batches,
        query_interval=args.interval,
    )
