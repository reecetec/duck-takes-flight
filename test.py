import pyarrow as pa
import pyarrow.flight as flight
import random
import time
from pyarrow._flight import FlightUnavailableError

# Connect to the Arrow Flight server
def connect_with_retry(max_attempts=5):
    for attempt in range(max_attempts):
        try:
            client = flight.connect("grpc://localhost:8815")
            return client
        except FlightUnavailableError:
            if attempt < max_attempts - 1:
                print(f"Connection attempt {attempt + 1} failed, retrying in 1 second...")
                time.sleep(1)
            else:
                raise

def query_data():
    query = "SELECT count(1) FROM concurrent_test"
    ticket = flight.Ticket(query.encode("utf-8"))

    # Send the query to the server and get the result
    reader = client.do_get(ticket)
    result_table = reader.read_all()
    print("Query Result:")
    print(result_table)

if __name__ == "__main__":
    print("Connecting to server...")
    client = connect_with_retry()
    print("Connected successfully!")
    query_data()
