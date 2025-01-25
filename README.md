# Duck Takes Flight

A high-performance data streaming system using DuckDB and Apache Arrow Flight.

## Components

1. **DuckDB Flight Server** (`duckdb_flight_server.py`): A server that exposes DuckDB through Arrow Flight protocol
2. **Data Loader** (`load_data.py`): Continuously generates and loads random data into DuckDB
3. **Query Client** (`query_data.py`): Executes continuous queries against the loaded data

## Setup

1. Install dependencies:
```bash
pip install duckdb pyarrow
```

2. Start the server:
```bash
python duckdb_flight_server.py
```

3. Start the data loader:
```bash
python load_data.py
```

4. Start the query client:
```bash
python query_data.py
```

## Data Schema

The system creates a table called `concurrent_test` with the following schema:
- `batch_id`: BIGINT
- `timestamp`: VARCHAR
- `value`: DOUBLE
- `category`: VARCHAR

## Features

- Persistent storage using DuckDB
- High-performance data transfer using Arrow Flight
- Continuous data loading and querying
- Memory-efficient batch processing
- Aligned Arrow buffers for optimal performance 