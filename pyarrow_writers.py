import os
from abc import abstractmethod
from typing import Any, Optional

import pyarrow.parquet as pq

from parquet_converter.core.types import Batch
from parquet_converter.utils.md5lib import generate_md5_checksums
from parquet_converter.writers.base_writers import BaseWriter


class PyArrowWriter(BaseWriter):
    """Abstract base class for PyArrow-based writers.

    Provides common functionality for all writers that use PyArrow as their backend.
    Subclasses implement format-specific writing logic. Both write_table and close must
    be implemented by subclasses to define the specific writing behavior.
    """

    @abstractmethod
    def write_table(self, batch: Batch) -> None:
        """Writes a batch of data using PyArrow.

        Args:
            batch (Batch): PyArrowBatch instance wrapping a PyArrow table.
        """

        pass

    @abstractmethod
    def close(self) -> None:
        """Finalizes writing and closes the PyArrow writer."""

        pass


class PyArrowParquetWriter(PyArrowWriter):
    """Parquet writer using PyArrow backend.

    The writer writes to a temporary file and moves it to the final location only on
    successful completion. After writing, it generates MD5 checksums for data integrity verification.

    Attributes:
        kwargs (dict[str, Any]): Additional arguments for PyArrow's ParquetWriter
        output_path (str): Final destination path for the parquet file.
        tmp_output_path (str): Temporary path (output_path + ".tmp") used during writing.
        schema (pyarrow.Schema): PyArrow schema defining column names and types.
        writer (pq.ParquetWriter): PyArrow ParquetWriter instance for writing data.
    """

    def __init__(self, kwargs: Optional[dict[str, Any]], output_path: str, schema: Any) -> None:
        """Initializes the PyArrow Parquet writer.

        Creates a PyArrow ParquetWriter instance configured with dictionary encoding
        and statistics generation enabled. The writer writes to a temporary file.

        Args:
            kwargs (Optional[dict[str, Any]]): Additional keyword arguments to pass
                to PyArrow's ParquetWriter constructor.
            output_path (str): Final destination path for the Parquet file. Parent
                directory must exist.
            schema (Any): PyArrow Schema object defining the structure and types of
                the output data.
        """

        super().__init__(kwargs, output_path, schema)

        self.writer: pq.ParquetWriter = pq.ParquetWriter(
            self.tmp_output_path, self.schema, use_dictionary=True, write_statistics=True, **self.kwargs
        )

    def write_table(self, batch: Batch) -> None:
        """Writes a batch of data to the Parquet file.

        Unwraps the batch to get the underlying PyArrow Table and writes it to the
        parquet file. This method can be called multiple times to write data
        incrementally as batches are processed.

        Args:
            batch (Batch): PyArrowBatch instance containing the data to write.
        """

        table = batch.unwrap()
        self.writer.write_table(table)

    def close(self) -> None:
        """Finalizes the parquet file and generates MD5 checksums.

        This method should be called exactly once after all batches have been written.
        The MD5 checksum is written to a separate .md5 file alongside the parquet file.
        """

        self.writer.close()
        if os.path.exists(self.tmp_output_path):
            os.replace(self.tmp_output_path, self.output_path)
        if os.path.exists(self.output_path):
            generate_md5_checksums(self.output_path)
