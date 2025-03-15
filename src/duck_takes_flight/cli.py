import argparse

from .logging import configure_logging
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
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Path to log file (if not specified, logs to console only)",
    )
    args = parser.parse_args()

    # Configure logging
    logger = configure_logging(
        level=args.log_level,
        log_file=args.log_file,
        component="duck-flight-server",
    )

    logger.info(
        f"Starting server on {args.host}:{args.port} with database {args.db_path}"
    )

    # Start the server
    serve(args.db_path, args.host, args.port, logger)


if __name__ == "__main__":
    main()
