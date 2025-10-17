from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

from parquet_converter.core.types import Batch

"""
TODO:
    - Implement "header" methods
"""


class BaseReader(ABC):
    """This is the abstract base class for all readers.

    Readers handle the input side of our conversion pipeline, reading raw files
    in batches for processing. Different reader implementations support
    different file formats and backends. All readers share common functionality for
    handling reader-specific kwargs and optional data filters. Subclasses must
    implement the batch_read method to define how data is actually read and batched.

    Attributes:
        kwargs (dict[str, Any]): backend-specific keyword arguments passed to the
            underlying read functions.
        filters (Optional[Any]): optional filter expressions to apply during reading
            for predicate pushdown (not yet implemented).
    """

    def __init__(self, kwargs: Optional[dict[str, Any]] = None, filters: Optional[Any] = None) -> None:
        """Initializes the reader with optional settings/filters.

        Args:
            kwargs (Optional[dict[str, Any]]): optional dictionary of keyword arguments
                to pass to the underlying read functions. Depends on the specific
                reader implementation and backend.
            filters (Optional[Any]): optional filter expressions during reading. Currently
                unused but reserved for future filtering support.
        """

        self.kwargs = kwargs if kwargs else {}
        self.filters = filters

    @abstractmethod
    def open(self, file_path: str, **kwargs) -> Any:
        """Opens the file and returns a file handler.

        Args:
            file_path (str): path to the file to open.
            **kwargs: additional keyword arguments for file opening.

        Returns:
            Any: backend-specific file handler object.
        """

        pass

    @abstractmethod
    def close(self, file_handler: Any, **kwargs) -> None:
        """Closes the file handler and performs cleanup.

        Args:
            file_handler (Any): backend-specific file handler to close.
            **kwargs: additional keyword arguments for cleanup operations.
        """

        pass

    @abstractmethod
    def batch_read(self, file_path: str, schema: Optional[Any] = None, **kwargs) -> Generator[Batch, None, None]:
        """Reads a file and yields data in batches.

        This is the core method that subclasses must implement. It should read the file
        at the given path, apply the optional schema, and yield data in batches for
        memory-efficient processing.

        Args:
            file_path (str): path to the file to read. Can be a local path, compressed
                file (.gz), or potentially a zip member (format: "archive.zip|member.csv").
            schema (Optional[Any]): optional backend-specific schema to apply during reading.
                This can enforce types and validate data. The exact type depends on the backend
                (ex. pyarrow.Schema for PyArrow readers).
            **kwargs: additional keyword arguments that may be needed by specific reader
                implementations.

        Yields:
            Batch: batches of data wrapped in the backend-independent Batch protocol.
        """

        pass
