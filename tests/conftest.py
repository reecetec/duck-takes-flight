"""
Common test fixtures for duck-takes-flight tests.
"""

import os
import random
import socket
import tempfile
import time
from contextlib import contextmanager
from multiprocessing import Process

import pyarrow as pa
import pytest

from duck_takes_flight.client import DuckDBFlightClient
from duck_takes_flight.server import serve


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
    """Fixture to start a Flight server for testing."""
    host = "localhost"
    port = find_free_port()  # Use a random free port to avoid conflicts

    # Start server in a separate process
    server_process = Process(target=serve, args=(temp_db_path, host, port), daemon=True)
    server_process.start()

    # Wait for server to start
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            # Try to connect to the server
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                if s.connect_ex(("localhost", port)) == 0:
                    break
        except:
            pass

        # If we've tried the maximum number of times, fail
        if attempt == max_attempts - 1:
            server_process.terminate()
            pytest.fail(f"Could not connect to server after {max_attempts} attempts")

        time.sleep(1)

    yield {"host": host, "port": port, "db_path": temp_db_path}

    # Cleanup
    server_process.terminate()
    server_process.join(timeout=1)

    # Force kill if still alive
    if server_process.is_alive():
        server_process.kill()


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
