import os
import subprocess
import tempfile
from typing import Any, BinaryIO, Generator, Optional

import pyarrow
import pyarrow.csv as pcsv

from parquet_converter.backends.pyarrow_backend import PyArrowBatch
from parquet_converter.readers.pyarrow_readers import PyArrowCsvReader


class GeneralizedCsvReader(PyArrowCsvReader):
    """Generalized CSV reader with support for skipfooter via preprocessing.
    
    When skipfooter > 0, uses Unix `head` command to efficiently remove footer
    rows without loading the entire file into memory. The preprocessed content
    is written to a temporary file, which is then streamed by PyArrow.
    
    Attributes:
        skipfooter (int): Number of rows to skip from the end of the file. Default is 0.
    """
    
    def __init__(
        self,
        skipfooter: int = 0,
        **kwargs
    ) -> None:
        """Initializes the generalized CSV reader.
        
        Args:
            skipfooter (int): Number of rows to skip from end of file. Default is 0.
                When skipfooter > 0, uses subprocess to efficiently remove footer rows.
            **kwargs: Additional arguments passed to PyArrowCsvReader:
                - kwargs: Backend-specific keyword arguments for PyArrow
                - filters: Optional filter expressions (not yet implemented)
                - block_size_mb: Size of each read block in MB (default 512)
                - options: Dict with 'read_options', 'parse_options', 'convert_options'
                - encoding: File encoding (default 'utf-8')
        
        Example options dict:
            {
                'read_options': {
                    'skip_rows': 2,
                    'skip_rows_after_names': 0,
                    'column_names': None,
                    'autogenerate_column_names': False
                },
                'parse_options': {
                    'delimiter': '|',
                    'quote_char': '"',
                    'escape_char': None,
                    'newlines_in_values': False
                },
                'convert_options': {
                    'null_values': ['', 'NULL', 'N.A.'],
                    'true_values': ['true', 'True', '1'],
                    'false_values': ['false', 'False', '0'],
                    'strings_can_be_null': True,
                    'timestamp_parsers': ['%Y%m%d', '%Y-%m-%d']
                }
            }
        """
        super().__init__(**kwargs)
        self.skipfooter = skipfooter
    
    def batch_read(
        self, file_handler: BinaryIO, schema: Optional[Any] = None, **kwargs
    ) -> Generator[PyArrowBatch, None, None]:
        """Reads a CSV file and yields data in batches.
        
        When skipfooter=0, uses standard streaming batch reading for memory efficiency.
        When skipfooter>0, preprocesses the file using Unix tools to remove footer rows,
        then streams the cleaned file.
        
        Args:
            file_handler (BinaryIO): File handler from open() method (binary mode).
            schema (Optional[Any]): Optional PyArrow schema for type enforcement and
                validation during reading.
            **kwargs: Additional keyword arguments (currently unused).
            
        Yields:
            PyArrowBatch: PyArrowBatch instances containing portions of the CSV data.
                Each batch represents approximately block_size bytes of data.
        """
        # If no footer to skip, use standard streaming approach
        if self.skipfooter == 0:
            yield from self._stream_read(file_handler, schema)
            return
        
        # If skipfooter > 0, preprocess with Unix tools
        yield from self._read_with_preprocessing(file_handler, schema)
    
    def _stream_read(
        self, file_handler: BinaryIO, schema: Optional[Any] = None
    ) -> Generator[PyArrowBatch, None, None]:
        """Standard streaming batch read without footer handling.
        
        This is the most memory-efficient approach, reading data in chunks without
        loading the entire file into memory.
        
        Args:
            file_handler (BinaryIO): File handler to read from.
            schema (Optional[Any]): Optional PyArrow schema for type enforcement.
            
        Yields:
            PyArrowBatch: Batches of data.
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
        
        # Create CSV stream reader
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
            if stream_reader is not None:
                stream_reader.close()
    
    def _read_with_preprocessing(
        self, file_handler: BinaryIO, schema: Optional[Any] = None
    ) -> Generator[PyArrowBatch, None, None]:
        """Preprocesses file to remove footer, then streams the result.
        
        Uses Unix `head -n -N` command which streams efficiently without
        loading the entire file into memory. The cleaned content is written
        to a temporary file which is then streamed by PyArrow.
        
        Args:
            file_handler (BinaryIO): File handler to read from.
            schema (Optional[Any]): Optional PyArrow schema for type enforcement.
            
        Yields:
            PyArrowBatch: Batches of data with footer rows removed.
        """
        # Create a temporary file for the preprocessed content
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
            
            try:
                # Use head -n -N to remove last N lines (streams efficiently)
                # Note: head reads the file in chunks, doesn't load it all into memory
                result = subprocess.run(
                    ['head', '-n', f'-{self.skipfooter}'],
                    stdin=file_handler,
                    stdout=tmp_file,
                    check=True,
                    stderr=subprocess.PIPE
                )
                
                # Close the temp file so we can reopen it for reading
                tmp_file.close()
                
                # Now open and stream the cleaned file
                with open(tmp_path, 'rb') as clean_file:
                    yield from self._stream_read(clean_file, schema)
            
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to preprocess file with head command: {e.stderr.decode()}") from e
            
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)


class BARRAReader(GeneralizedCsvReader):
    """Specialized reader for MSCI BARRA CSV files.
    
    Pre-configured with BARRA-specific defaults:
    - Pipe delimiter (|)
    - Skip 1 footer row
    - Skip 2 header rows (can be overridden per table)
    
    This reader is a convenience wrapper around GeneralizedCsvReader with
    BARRA-specific defaults. All options can still be overridden via config.
    """
    
    def __init__(self, **kwargs):
        """Initializes BARRA reader with typical defaults.
        
        Args:
            **kwargs: Arguments passed to GeneralizedCsvReader. Common overrides:
                - skipfooter: Override default of 1
                - options['read_options']['skip_rows']: Override default of 2
                - options['parse_options']['delimiter']: Override default of '|'
                - encoding: Override default of 'utf-8'
        """
        # Set BARRA-specific defaults
        if 'skipfooter' not in kwargs:
            kwargs['skipfooter'] = 1
        
        # Ensure options dict exists
        if 'options' not in kwargs:
            kwargs['options'] = {}
        
        # Set default read_options
        if 'read_options' not in kwargs['options']:
            kwargs['options']['read_options'] = {}
        if 'skip_rows' not in kwargs['options']['read_options']:
            kwargs['options']['read_options']['skip_rows'] = 2
        
        # Set default parse_options
        if 'parse_options' not in kwargs['options']:
            kwargs['options']['parse_options'] = {}
        if 'delimiter' not in kwargs['options']['parse_options']:
            kwargs['options']['parse_options']['delimiter'] = '|'
        
        super().__init__(**kwargs)
