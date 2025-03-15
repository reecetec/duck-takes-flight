import argparse

from .server import serve


def main():
    parser = argparse.ArgumentParser(description="Duck Takes Flight CLI")
    parser.add_argument(
        "--db-path",
        type=str,
        default="duck_flight.db",
        help="Path to the DuckDB database file",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host to connect to",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8815,
        help="Port to connect to",
    )
    args = parser.parse_args()

    serve(args.db_path, args.host, args.port)


if __name__ == "__main__":
    main()
