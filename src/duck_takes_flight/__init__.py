from importlib.metadata import version

__version__ = version("duck-takes-flight")

from .client import DuckDBFlightClient
from .server import DuckDBFlightServer, serve

__all__ = ["DuckDBFlightClient", "DuckDBFlightServer", "serve"]
