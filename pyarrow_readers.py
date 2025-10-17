import io
from abc import abstractmethod
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, Generator, Optional
from zipfile import ZipFile

import pyarrow
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from parquet_converter.backends.pyarrow_backend import PyArrowBatch
from parquet_converter.core.types import Batch
from parquet_converter.readers.base_readers import BaseReader
from parquet_converter.utils.file_utils import open_file_raw


class PyArrowReader(BaseReader):
    """This is the abstract base class for PyArrow-based readers.

    Provides common functionality for all readers that use PyArrow as their backend.
    Subclasses of this will implement format-specific reading logic.
    """

    @abstractmethod
    def open(self, file_path: str, **kwargs) -> Any:
        """Opens the file and returns a file handler.

        Args:
            file_path (str): Path to the file to open.
            **kwargs: Additional keyword arguments for file opening.

        Returns:
            Any: Backend-specific file handler object.
        """
        pass

    @abstractmethod
    def close(self, file_handler: Any, **kwargs) -> None:
        """Closes the file handler and performs cleanup.

        Args:
            file_handler (Any): Backend-specific file handler to close.
            **kwargs: Additional keyword arguments for cleanup operations.
        """
        pass

    @abstractmethod
    def batch_read(self, file_path: str, schema: Optional[Any] = None, **kwargs) -> Generator[Batch, None, None]:
        """Reads a file and yields PyArrow-backed batches.

        Args:
            file_path (str): path to the file to read.
            schema (Optional[Any]): optional PyArrow schema for type enforcement.
            **kwargs: additional reader-specific arguments.

        Yields:
            Batch: PyArrowBatch instances wrapping PyArrow tables.
        """

        pass


class PyArrowCsvReader(PyArrowReader):
    """CSV reader using PyArrow backend.

    Reads CSV files in batches using PyArrow.
    Supports:
    - Gzipped CSV files (.csv.gz)
    - CSV files within zip files (via is_zip flag; "zip_path|member" format)
    - Configurable read, parse, and convert options via the config
    - Custom block sizes for batch streaming
    - Schema enforcement and type conversion

    Attributes:
        kwargs (dict[str, Any]): additional arguments to pass to PyArrow's open_csv.
        filters (Optional[Any]): filter expressions (currently unused).
        block_size (int): size in bytes for each read block/batch.
        encoding (str): file encoding to use when opening files. Defaults to 'utf-8'.
        options (dict[str, Any]): dictionary containing read_options, parse_options,
            and convert_options sub-dictionaries for PyArrow CSV reading.
    """

    def __init__(
        self,
        kwargs: Optional[dict[str, Any]] = None,
        filters: Optional[Any] = None,
        block_size_mb: float = 512,
        options: Optional[dict[str, Any]] = None,  # options dict from config
        encoding: Optional[str] = None,
    ) -> None:
        """Initializes the PyArrow CSV reader.

        Args:
            kwargs (Optional[dict[str, Any]]): additional keyword arguments to pass
                to PyArrow's open_csv function.
            filters (Optional[Any]): filter expressions (currently unused).
            block_size_mb (float): size of each read block in megabytes. Determines
                batch size. Default is 512 MB.
            options (Optional[dict[str, Any]]): dictionary containing PyArrow CSV
                reading options with keys:
                - 'read_options': dict for pcsv.ReadOptions (skip_rows, block_size, etc.)
                - 'parse_options': dict for pcsv.ParseOptions (delimiter, quote_char, etc.)
                - 'convert_options': dict for pcsv.ConvertOptions (null_values, timestamp_parsers, etc.)
            encoding (Optional[str]): file encoding to use (e.g., 'utf-8', 'latin-1'). If None,
                defaults to 'utf-8'.
        """

        super().__init__(kwargs, filters)
        self.kwargs = kwargs if kwargs else {}
        self.filters = filters
        self.encoding = encoding if encoding is not None else "utf-8"

        # add a round to the block size to avoid issues with decimal block_size_mb
        self.block_size = round(1_048_576 * block_size_mb)

        # store options for read/parse/convert
        self.options = options if options else {}

    def open(self, file_path: str, mode: str = "rb", is_zip: bool = False) -> tuple:
        """Opens the CSV file and returns a file handler and temp directory.

        Handles both regular CSV files and CSV files within zip archives. For zip files,
        extracts to a temporary directory since PyArrow requires seekable files.

        Args:
            file_path (str): Path to the CSV file. For zip files, format is "zip_path|member".
            mode (str): File open mode. Default is "rb" (read binary).
            is_zip (bool): Whether the file is within a zip archive. Default is False.

        Returns:
            tuple: (file_handler, temp_dir) where file_handler is a BinaryIO object
                and temp_dir is a TemporaryDirectory (or None for regular files).
        """
        if is_zip:
            zip_path, csv_filename = file_path.split("|", 1)
            temp_dir = TemporaryDirectory(dir="/tmp")
            zip_file = None
            try:
                zip_file = ZipFile(zip_path, "r")
                extracted_path = zip_file.extract(csv_filename, temp_dir.name)
                zip_file.close()
                zip_file = None
                csv_path = extracted_path
            except:
                if zip_file is not None:
                    zip_file.close()
                temp_dir.cleanup()
                raise
        else:
            csv_path = file_path
            temp_dir = None

        # Open the file in binary mode with compression support
        file_handler = open_file_raw(csv_path, mode=mode, encoding=self.encoding)

        return file_handler, temp_dir

    def close(self, file_handler: BinaryIO, temp_dir: Optional[TemporaryDirectory] = None) -> None:
        """Close the file handler and perform cleanup.

        Args:
            file_handler (BinaryIO): The file handler to close.
            temp_dir (Optional[TemporaryDirectory]): The temporary directory to clean up (if file was from zip).
        """
        file_handler.close()
        if temp_dir is not None:
            temp_dir.cleanup()

    def batch_read(
        self, file_handler: BinaryIO, schema: Optional[Any] = None, **kwargs
    ) -> Generator[PyArrowBatch, None, None]:
        """Reads a CSV file and yields data in batches.

        Streams CSV data in batches for memory-efficient processing using PyArrow's
        streaming CSV reader. The file_handler should be obtained via the open() method.

        Args:
            file_handler (BinaryIO): File handler from open() method (binary mode).
            schema (Optional[Any]): Optional PyArrow schema for type enforcement and
                validation during reading.
            **kwargs: Additional keyword arguments (currently unused).

        Yields:
            PyArrowBatch: PyArrowBatch instances containing portions of the CSV data. Each
                batch represents approximately block_size bytes of data.
        """
        # Build ReadOptions
        read_options_dict = self.options.get("read_options", {})
        if "block_size" not in read_options_dict:
            read_options_dict["block_size"] = self.block_size
        read_options = pcsv.ReadOptions(**read_options_dict)

        # Build ParseOptions
        parse_options_dict = self.options.get("parse_options", {})
        parse_options = pcsv.ParseOptions(**parse_options_dict) if parse_options_dict else None

        # Build ConvertOptions
        convert_options_dict = self.options.get("convert_options", {})
        if schema is not None:
            convert_options_dict["column_types"] = schema
        if "timestamp_parsers" not in convert_options_dict:
            convert_options_dict["timestamp_parsers"] = ["%Y%m%d"]
        convert_options = pcsv.ConvertOptions(**convert_options_dict)

        # Create CSV stream reader from raw file handler
        stream_reader = None
        try:
            stream_reader = pcsv.open_csv(
                file_handler,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
                **self.kwargs,
            )

            for batch in stream_reader:
                table = pyarrow.Table.from_batches([batch])
                yield PyArrowBatch(table)
        finally:
            # Close stream reader if it exists
            if stream_reader is not None:
                stream_reader.close()


class PyArrowParquetReader(PyArrowReader):
    """Parquet reader using PyArrow backend.

    Reads Parquet files in batches using PyArrow's parquet reader.

    Attributes:
        kwargs (dict[str, Any]): additional arguments to pass to PyArrow's ParquetFile.
        filters (Optional[Any]): filter expressions (currently unused).
        batch_size (int): number of rows per batch when iterating. We have decided on 100,000
            after testing many sizes.
        encoding (Optional[str]): file encoding (not typically used for binary parquet files).
            Defaults to None.
    """

    def __init__(
        self,
        kwargs: Optional[dict[str, Any]] = None,
        filters: Optional[Any] = None,
        batch_size: int = 100_000,
        encoding: Optional[str] = None,
    ) -> None:
        """Initializes the PyArrow Parquet reader.

        Args:
            kwargs (Optional[dict[str, Any]]): Additional keyword arguments to pass to ParquetFile.
            filters (Optional[Any]): Filter expressions (currently unused).
            batch_size (int): Number of rows per batch. Default is 100,000.
            encoding (Optional[str]): file encoding to use (typically not used for parquet files).
                If None, defaults to None.
        """
        super().__init__(kwargs, filters)
        self.batch_size = batch_size
        self.encoding = encoding

    def open(self, file_path: str, **kwargs) -> BinaryIO:
        """Opens the parquet file and returns the file handler.

        Args:
            file_path (str): Path to the parquet file.
            **kwargs: Additional keyword arguments.

        Returns:
            pyarrow.parquet.ParquetFile: The opened parquet file handler.
        """
        # Parquet files are binary, so encoding not used
        return open_file_raw(file_path, mode="rb")

    def close(self, file_handler: BinaryIO, **kwargs) -> None:
        """Close the file handler and perform cleanup.

        Args:
            file_handler (BinaryIO): The file handler to close.
            **kwargs: Additional keyword arguments (currently unused).
        """
        file_handler.close()

    def batch_read(
        self, file_handler: BinaryIO, schema: Optional[Any] = None, **kwargs
    ) -> Generator[PyArrowBatch, None, None]:
        """Reads a Parquet file and yields data in batches.

        Iterates through the parquet file in batches for memory-efficient processing.
        The file_handler should be obtained via the open() method.

        Args:
            file_handler (BinaryIO): File handler from open() method (binary mode).
            schema (Optional[Any]): Optional schema (currently unused for parquet reading).
            **kwargs: Additional keyword arguments (currently unused).

        Yields:
            PyArrowBatch: PyArrowBatch instances containing portions of the parquet data.
                Each batch contains up to batch_size rows.
        """
        parquet_file = pq.ParquetFile(file_handler, **self.kwargs)

        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            table = pyarrow.Table.from_batches([batch])
            yield PyArrowBatch(table)


class BloombergReader(PyArrowReader):
    """Bloomberg file reader using PyArrow backend.

    Reads Bloomberg-format files in batches. Bloomberg files have a specific format with
    FIELDS and DATA sections separated by markers.

    Attributes:
        kwargs (dict[str, Any]): Additional arguments.
        filters (Optional[Any]): Filter expressions (currently unused).
        block_size (int): Size in bytes for each read block/batch.
        encoding (str): file encoding to use when opening files. Defaults to 'utf-8'.
    """

    def __init__(
        self,
        kwargs: Optional[dict[str, Any]] = None,
        filters: Optional[Any] = None,
        block_size_mb: float = 512,
        encoding: Optional[str] = None,
    ) -> None:
        """Initializes the Bloomberg reader.

        Args:
            kwargs (Optional[dict[str, Any]]): Additional keyword arguments.
            filters (Optional[Any]): Filter expressions (currently unused).
            block_size_mb (float): Size of each read block in megabytes. Default is 512 MB.
            encoding (Optional[str]): file encoding to use (e.g., 'utf-8', 'latin-1'). If None,
                defaults to 'utf-8'.
        """
        super().__init__(kwargs, filters)
        self.kwargs = kwargs if kwargs else {}
        self.filters = filters
        self.block_size = round(1_048_576 * block_size_mb)
        self.encoding = encoding if encoding is not None else "utf-8"

    def open(self, file_path: str, **kwargs) -> BinaryIO:
        """Opens the Bloomberg file and returns a file handler.

        Handles both regular and gzipped Bloomberg files.

        Args:
            file_path (str): Path to the Bloomberg file.
            **kwargs: Additional keyword arguments.

        Returns:
            Union[gzip.GzipFile, io.TextIOWrapper]: The opened file handler.
        """
        # Open in text mode for Bloomberg files since we need line-by-line reading
        return open_file_raw(file_path, mode="rt", encoding=self.encoding)

    def close(self, file_handler: BinaryIO, **kwargs) -> None:
        """Closes the file handler.

        Args:
            file_handler (Union[gzip.GzipFile, io.TextIOWrapper]): The file handler to close.
            **kwargs: Additional keyword arguments.
        """
        file_handler.close()

    def batch_read(
        self, file_handler: BinaryIO, schema: Optional[Any] = None, **kwargs
    ) -> Generator[PyArrowBatch, None, None]:
        """Reads a Bloomberg file and yields data in batches.

        Parses Bloomberg format files which contain FIELDS and DATA sections.
        The first 3 columns (sec_desc, rcode, nfields) are always present, followed
        by fields defined in the FIELDS section. The file_handler should be obtained
        via the open() method.

        Args:
            file_handler (BinaryIO): File handler from open() method (text mode).
            schema (Optional[Any]): Optional PyArrow schema for type enforcement.
            **kwargs: Additional keyword arguments (currently unused).

        Yields:
            PyArrowBatch: PyArrowBatch instances containing portions of the Bloomberg data.
                Each batch represents approximately block_size bytes of data.
        """
        # The first 3 columns are always these, not specified in FIELDS section
        field_names = ["sec_desc", "rcode", "nfields"]
        data_lines = []
        current_batch_size = 0
        is_fields = False
        is_data = False

        parse_options = pcsv.ParseOptions(delimiter="|")

        for line in file_handler:
            line = line.rstrip()

            # Handle section markers
            if line == "START-OF-FIELDS":
                is_fields = True
                is_data = False
                continue
            elif line == "END-OF-FIELDS":
                is_fields = False
                continue
            elif line == "START-OF-DATA":
                is_data = True
                is_fields = False
                continue
            elif line == "END-OF-DATA":
                # Yield any remaining data
                if data_lines:
                    data_buffer = io.BytesIO("\n".join(data_lines).encode("utf-8"))
                    read_options = pcsv.ReadOptions(column_names=field_names)
                    with pcsv.open_csv(
                        data_buffer,
                        read_options=read_options,
                        parse_options=parse_options,
                        convert_options=convert_options,
                    ) as stream_reader:
                        for batch in stream_reader:
                            table = pyarrow.Table.from_batches([batch])
                            yield PyArrowBatch(table)
                    data_buffer.close()
                    data_lines = []
                    current_batch_size = 0
                is_data = False
                continue

            # Process fields section
            if is_fields:
                # Skip comment lines (starting with #) and blank lines
                if line.startswith("#") or line == "":
                    continue
                # Handle duplicate field names
                if line in field_names:
                    count = field_names.count(line)
                    field_names.append(f"{line}_{count}")
                else:
                    field_names.append(line)

            # Process data section
            elif is_data:
                # Remove trailing pipe delimiter if present
                if line.endswith("|"):
                    line = line[:-1]

                # On first data line, set up convert options
                if not data_lines:
                    # Now set up convert options with correct field count
                    # if not schema:
                    #     schema = {col: pyarrow.string() for col in field_names}
                    convert_options = pcsv.ConvertOptions(
                        column_types=schema, strings_can_be_null=True, null_values=["N.A."]
                    )

                data_lines.append(line)
                current_batch_size += len(line)

                # When we reach the block size, yield a batch
                if current_batch_size >= self.block_size:
                    data_buffer = io.BytesIO("\n".join(data_lines).encode("utf-8"))
                    read_options = pcsv.ReadOptions(column_names=field_names)
                    with pcsv.open_csv(
                        data_buffer,
                        read_options=read_options,
                        parse_options=parse_options,
                        convert_options=convert_options,
                    ) as stream_reader:
                        for batch in stream_reader:
                            table = pyarrow.Table.from_batches([batch])
                            yield PyArrowBatch(table)
                    data_buffer.close()
                    data_lines = []
                    current_batch_size = 0
