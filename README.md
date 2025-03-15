# Duck Takes Flight

A high-performance data streaming system using DuckDB and Apache Arrow Flight.

## Installation

```bash
pip install duck-takes-flight
```

## Usage

### Starting the Server

The package provides a command-line interface (CLI) to start the server:

```bash
# Start with default settings (localhost:8815, duck_flight.db)
duck-flight

# Start with custom settings
duck-flight --host 0.0.0.0 --port 9000 --db-path my_database.db
```

#### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--host` | Host to bind the server to | `localhost` |
| `--port` | Port to bind the server to | `8815` |
| `--db-path` | Path to the DuckDB database file | `duck_flight.db` |

### Using the Client

You can interact with the server using the `DuckDBFlightClient` class:

```python
from duck_takes_flight import DuckDBFlightClient
import pyarrow as pa

# Connect to the server
client = DuckDBFlightClient(host="localhost", port=8815)

# Execute a query
result = client.execute_query("SELECT * FROM my_table")
print(result)

# Upload data
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "value": [10.1, 20.2, 30.3]
}
table = pa.Table.from_pydict(data)
client.upload_data("my_table", table)

# Execute a custom action (e.g., create a table)
client.execute_action("query", "CREATE TABLE IF NOT EXISTS new_table (id INT, name VARCHAR)")
```

### Client API

#### `DuckDBFlightClient(host="localhost", port=8815, max_attempts=5)`

Creates a new client connection to the server.

- `host`: The hostname of the server
- `port`: The port of the server
- `max_attempts`: Number of connection attempts before failing

#### `execute_query(query)`

Executes a SQL query and returns the results as a PyArrow Table.

#### `upload_data(table_name, table)`

Uploads a PyArrow Table to the server with the given table name.

#### `execute_action(action_type, body=None)`

Executes a custom action on the server.
- `action_type`: The type of action (e.g., "query")
- `body`: The body of the action (e.g., a SQL statement)

## Examples

### Create a table and insert data

```python
from duck_takes_flight import DuckDBFlightClient
import pyarrow as pa

# Connect to the server
client = DuckDBFlightClient()

# Create a table
client.execute_action("query", """
    CREATE TABLE users (
        id INTEGER,
        name VARCHAR,
        email VARCHAR
    )
""")

# Insert data
data = {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"]
}
table = pa.Table.from_pydict(data)
client.upload_data("users", table)

# Query the data
result = client.execute_query("SELECT * FROM users")
print(result)
```

### Query with filters and aggregations

```python
from duck_takes_flight import DuckDBFlightClient

# Connect to the server
client = DuckDBFlightClient()

# Query with filter
filtered = client.execute_query("SELECT * FROM users WHERE id > 1")
print(filtered)

# Query with aggregation
aggregated = client.execute_query("SELECT COUNT(*) as count FROM users")
print(aggregated)
```

## Features

- Persistent storage using DuckDB
- High-performance data transfer using Arrow Flight
- Memory-efficient batch processing
- Aligned Arrow buffers for optimal performance
- Automatic schema inference
- Retry logic for robust connections
