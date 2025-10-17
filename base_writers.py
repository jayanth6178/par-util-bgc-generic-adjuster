from abc import ABC, abstractmethod
from typing import Any, Optional

from parquet_converter.core.types import Batch


class BaseWriter(ABC):
    """Abstract base class for all data writers.

    Writers handle the output side of the conversion pipeline, writing processed data
    batches to files with proper schema enforcement. Different writer implementations
    support different file formats and backends.

    All writers use a temporary file strategy: data is written to a .tmp file during
    processing and atomically moved to the final path on successful completion. This
    prevents partial/corrupted files from being created if processing fails.

    Attributes:
        kwargs (dict[str, Any]): Backend-specific keyword arguments passed to the
            underlying write functions.
        output_path (str): Final destination path for the output file.
        tmp_output_path (str): Temporary path where data is written during processing.
        schema (Any): Backend-specific schema object defining the structure and types
            of data to be written.
        writer (Optional[Any]): Backend-specific writer instance.
    """

    def __init__(self, kwargs: Optional[dict[str, Any]], output_path: str, schema: Any) -> None:
        """Initializes the writer with output configuration.

        Sets up the writer with the destination path, schema, and any backend-specific
        options. Creates the temporary output path by appending ".tmp" to the final path.

        Args:
            kwargs (Optional[dict[str, Any]]): Optional dictionary of keyword arguments
                to pass to the underlying write functions. Contents depend on the specific
                writer implementation and backend.
            output_path (str): Final destination path for the output file. The parent
                directory must exist or be created before writing.
            schema (Any): Backend-specific schema object defining the structure and types
                of the output data. Type depends on the backend.
        """

        self.kwargs = kwargs if kwargs else {}
        self.output_path: str = output_path
        self.tmp_output_path: str = self.output_path + ".tmp"
        self.schema: Any = schema
        self.writer: Optional[Any] = None

    @abstractmethod
    def write_table(self, batch: Batch) -> None:
        """Writes a batch of data to the output file.

        This is the core method that subclasses must implement. It should write the
        batch's data to the temporary output file using the backend-specific writer.
        The method will be called multiple times during processing as batches are
        produced.

        Args:
            batch (Batch): Batch of data to write, wrapped in the backend-independent
                Batch protocol.
        """

        pass

    def close(self) -> None:
        """Finalizes writing and close the output file.

        This method should be called after all batches have been written. Subclasses typically
        override this to:
        1. Close the backend writer
        2. Move the temporary file to the final output path (atomic write)
        3. Perform any post-processing
        """

        pass
