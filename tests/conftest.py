"""
Common test fixtures for duck-takes-flight tests.
"""

import os
import socket
import tempfile
import threading
import time

import pyarrow as pa
import pytest

from duck_takes_flight.client import DuckDBFlightClient
from duck_takes_flight.server import DuckDBFlightServer


def find_free_port():
    """Find a free port to use for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture
def temp_db_path():
    """Fixture to create a temporary database file."""
    # Create a temporary directory instead of a file
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test.db")
        yield db_path


@pytest.fixture
def flight_server(temp_db_path):
    """Fixture to start a Flight server for testing.

    This uses threading instead of multiprocessing to avoid segmentation faults in CI.
    """
    host = "localhost"
    port = find_free_port()  # Use a random free port to avoid conflicts
    location = f"grpc://{host}:{port}"

    # Create the server
    server = DuckDBFlightServer(location=location, db_path=temp_db_path)

    # Start server in a separate thread
    server_thread = threading.Thread(target=server.serve, daemon=True)
    server_thread.start()

    # Wait for server to start
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            # Try to connect to the server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex((host, port)) == 0:
                    break
        except Exception:
            pass

        # If we've tried the maximum number of times, fail
        if attempt == max_attempts - 1:
            pytest.fail(f"Could not connect to server after {max_attempts} attempts")

        time.sleep(1)

    yield {"host": host, "port": port, "db_path": temp_db_path, "server": server}

    # No need to explicitly clean up the thread as it's a daemon thread


@pytest.fixture
def flight_client(flight_server):
    """Fixture to create a Flight client connected to the test server."""
    client = DuckDBFlightClient(
        host=flight_server["host"], port=flight_server["port"], max_attempts=5
    )
    return client


@pytest.fixture
def sample_table():
    """Fixture to create a sample PyArrow table for testing."""
    data = {
        "id": pa.array([1, 2, 3, 4, 5]),
        "name": pa.array(["Alice", "Bob", "Charlie", "Dave", "Eve"]),
        "value": pa.array([10.1, 20.2, 30.3, 40.4, 50.5]),
        "active": pa.array([True, False, True, True, False]),
    }
    return pa.Table.from_pydict(data)
