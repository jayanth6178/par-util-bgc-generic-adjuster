import importlib
import logging
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from parquet_converter.utils.config import Config
from parquet_converter.utils.file_utils import create_dir
from parquet_converter.utils.finder import FileFinder
from parquet_converter.utils.manifest import Manifest

from parquet_converter.core.types import RecordSchema
from parquet_converter.core.ops_registry import get_ops
OPS = get_ops() 

def import_class(class_path):
    """Dynamically imports a class from a module path relative to parquet_converter.

    Takes in a dot-separated class path as outlined in the config (ex: 'readers.PyArrowCsvReader')
    and imports that class from the parquet_converter package. This allows us to have flexible
    class instantiation for readers, writers, and adjusters.

    Args:
        class_path (str): dot-separated path to the class, relative to parquet_converter
            (ex: 'readers.PyArrowCsvReader' or 'adjusters.metadata_adjusters.StandardMetadataAdjuster').

    Returns:
        type: the imported class object.
    """

    # Split the path into parts
    parts = class_path.split(".")

    # The last part is the class name
    class_name = parts[-1]

    # Everything before the last part is the module path (relative to parquet_converter)
    module_path = ".".join(parts[:-1])

    # Construct the full module path
    full_module_path = f"parquet_converter.{module_path}"

    # Use importlib to import the module
    module = importlib.import_module(full_module_path)

    # Get and return the class from the module
    return getattr(module, class_name)


def format_time(seconds):
    """Format elapsed time in seconds to a human-readable string.

    Converts seconds into the most appropriate unit (seconds, minutes and seconds,
    or hours, minutes and seconds) for readability. This is used in logging statements.

    Args:
        seconds (float): elapsed time in seconds.

    Returns:
        str: formatted time string (ex: '1h 30m 45.123s')
    """

    seconds = float(seconds)
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {seconds:.3f}s"
    hours, minutes = divmod(int(minutes), 60)
    return f"{hours}h {minutes}m {seconds:.3f}s"


class BaseAdministrator(ABC):
    """Abstract base class for administrators, responsible for managing/orchestrating all processes.

    Administrators coordinate and manage this entire pipeline: finding files, reading data,
    applying adjustments, and writing output. Different administrator implementations can
    provide different methods/strategies for doing so. Again, the abstraction allows us to have
    different game plans for different vendors/datasets
    """

    @abstractmethod
    def process(self) -> None:
        """Process the conversion for the target raw file"""
        pass


class Administrator(BaseAdministrator):
    """The main administrator for orchestrating parquet conversion of raw files.

    This administrator handles the entire conversion pipeline:
    1. Locates raw files based on the patterns and date ranges
    2. Reads the data in batches using the specified reader
    3. Adds metadata columns according to the specified adjuster
    4. Writes the output to parquet using the specified writer

    Note that this administrator supports both aggregate mode (multiple files to one output)
    and per-file mode (one file to one output).

    Attributes:
        logger (logging.Logger): logger instance for log messages.
        config (Config): config object containing all desired settings.
        table (str): name of the table being processed.
        basedir (str): base directory for all output paths.
        config_basedir (str): config's basedir for manifest paths.
        manifest_template (str): template path for manifest files.
        output_template (str): template path for output parquet files.
        writer_kwargs (Dict[str, Any]): additional keyword arguments to pass to the writer.
        file_type (str): type of file pattern.
        extract_vars (Dict[str, Any]): variables to extract from filenames via regex.
        aggregate (bool): whether or not to aggregate multiple files into one output.
        raw_file_templates (List[str]): file path templates for finding raw files.
        num_files (int): count of files processed.
        num_batches (int): count of batches processed.
        num_rows (int): count of rows processed.
        sum_file_seconds (float): total time spent reading and writing files.
        reader: instance of the desired reader class.
        adjuster: instance of the desired adjuster class.
    """

    def __init__(self, logger: logging.Logger, config_path: str, table: str, output_override: Optional[str] = None) -> None:
        """Initializes the administrator with settings from the config and sets up reader/adjuster.

        Loads the config, extracts table-specific settings, and instantiates
        the reader and adjuster classes specified in the config.

        Args:
            logger (logging.Logger): logger for messages and debugging.
            config_path (str): path to the config file.
            table (str): name of the table to process (must exist in config).
            output_override (Optional[str]): optional output directory path override.
                Output will be written to '{output_override}/{table}_{YYYYMMDD}.parq'.
                Useful for testing without modifying the config file.
        """

        self.logger = logger
        self.config = Config.from_yaml(config_path)
        self.table = table

        # Get table config
        table_config = self.config.get_table_config(table)

        # all setting needed for conversion
        # Keep config's basedir for manifests (shared schema definitions)
        self.config_basedir = self.config.output.basedir
        
        self.manifest_template = self.config.manifest.file_template
        self.output_template = table_config.output_file_template or self.config.output.file_template

        # Handle output_override - simply use the provided path with default template
        if output_override is not None:
            self.basedir = ""
            self.output_template = os.path.join(output_override, "{table}_{YYYYMMDD}.parq")
            self.logger.info(f"Output override: writing to '{self.output_template}'")
        else:
            # No override - use config's basedir
            self.basedir = self.config.output.basedir

        self.writer_kwargs = self.config.output.writer_kwargs
        self.file_type = self.config.input.file_type
        self.extract_vars = self.config.extract_vars
        self.aggregate = table_config.aggregate
        self.raw_file_templates = table_config.raw_files
        self.num_files = 0
        self.num_batches = 0
        self.num_rows = 0
        self.sum_file_seconds = 0.0  # sum of per-file elapsed (read+write) times

        # setup reader
        reader_name = self.config.input.reader
        reader_cls = import_class(f"readers.{reader_name}")
        reader_kwargs = {"kwargs": self.config.input.kwargs}
        if self.config.input.options:
            reader_kwargs["options"] = self.config.input.options
        if self.config.input.encoding is not None:
            reader_kwargs["encoding"] = self.config.input.encoding
        self.reader = reader_cls(**reader_kwargs)

        # setup adjuster
        adjust_name = self.config.output.adjuster
        adjuster_cls = import_class(f"adjusters.{adjust_name}")
        self.adjuster = adjuster_cls(self.config.output.metadata)


    def _validate_schema_against_batch(
        self, logical_schema: Any, batch: Any, file_path: str, column_operations: dict[str, str]
    ) -> tuple[Any, set[str]]:
        """
        Validate schema against batch, handling column operations.

        Column operations:
        - "source_required": Column must exist in source (error if missing)
        - "source_optional": Column is optional (keep in schema even if missing, will be filled with nulls)
        - "output_ignored": Column should be removed from output even if present in source

        Args:
            logical_schema (RecordSchema): RecordSchema from manifest
            batch (Batch): Batch object with actual data
            file_path (str): Path to file being processed (for logging)
            column_operations (dict[str, str]): Mapping of column names to their operations

        Returns:
            tuple[RecordSchema, set[str]]: Validated RecordSchema and set of optional missing columns
        """

        # Get column names from the batch - unwrap to access PyArrow table
        table = batch.unwrap()
        batch_columns = set(table.column_names)
        schema_columns = set(logical_schema.names())

        # Find columns in schema that are not in the batch
        missing_columns = schema_columns - batch_columns

        # Process missing columns based on their operations
        required_missing = []

        for col in missing_columns:
            operation = column_operations.get(col, "source_required")
            if operation == "source_required":
                required_missing.append(col)
            # source_optional columns should not be in the reading schema at all
            # output_ignored columns don't matter if missing

        # Error out if required columns are missing
        if required_missing:
            raise ValueError(
                f"Schema validation failed for '{file_path}': "
                f"Required columns missing from raw file: {sorted(required_missing)}. "
                f"These columns are marked as 'source_required' and must be present in the source file."
            )

        # Filter the schema:
        # 1. Remove columns marked as "output_ignored"
        # 2. Keep "source_required" columns (all present since we error if missing)
        # Note: source_optional columns are not in the reading schema
        validated_fields = []
        for field in logical_schema.fields:
            operation = column_operations.get(field.name, "source_required")

            # Skip output_ignored columns
            if operation == "output_ignored":
                self.logger.debug(f"Ignoring column '{field.name}' (marked as output_ignored)")
                continue

            # Keep source_required columns (they must all be present)
            validated_fields.append(field)

        return RecordSchema(fields=tuple(validated_fields)), set()

    def _get_schema_and_writer(
        self,
        format_dict: Dict[str, Any],
        validation: bool = True,
        file_path: Optional[str] = None,
        is_zip: bool = False,
    ) -> Tuple[Any, Any, Any, set[str]]:
        """Set up schema and writer, optionally with validation against first batch of file.

        This method creates the schema and writer objects needed for the conversion process.
        When validation is enabled, it reads the first batch of the specified file to validate
        that all schema columns exist in the data, handling column operations and logging warnings.

        Args:
            format_dict (Dict[str, Any]): Dictionary for formatting manifest and output paths.
                Should contain template variables like 'table', 'date', 'file_name', etc.
            validation (bool): Whether to validate schema against first batch. Default is True.
                When True, file_path must be provided.
            file_path (Optional[str]): Path to the file for validation. Required when validation=True.
                For zip files, format should be "zip_path|member".
            is_zip (bool): Whether the file is within a zip archive. Default is False.
                Used only when validation=True.

        Returns:
            Tuple[Any, Any, Any, set[str]]: Tuple of (output_schema, reading_schema, writer, optional_missing_cols) where:
                - output_schema is the RecordSchema for output (includes source_optional, excludes output_ignored)
                - reading_schema is the RecordSchema to use for reading (excludes source_optional)
                - writer is the configured writer instance (uses output_schema)
                - optional_missing_cols is a set of source_optional column names to add as nulls

        Raises:
            ValueError: If validation=True but file_path is None.
        """
        self.logger.debug(f"Resolving manifest and output paths using the following format dictionary: {format_dict}")

        # Use config's basedir for manifest (shared schema definition)
        manifest_template = os.path.join(self.config_basedir, self.manifest_template)
        self.logger.debug(f"Template manifest path: {manifest_template}")
        manifest_path = manifest_template.format(**format_dict)
        self.logger.debug(f"Resolved manifest path: {manifest_path}")

        # Use potentially overridden basedir for output files
        full_output_template = os.path.join(self.basedir, self.output_template)
        self.logger.debug(f"Template output path: {full_output_template}")
        output_path = full_output_template.format(**format_dict)
        self.logger.debug(f"Resolved output path: {output_path}")

        # Get logical schemas with column operations
        logical_schema, column_operations = Manifest.get_schema_with_operations(manifest_path)
        metadata_schema = Manifest.get_metadata_schema(self.config.output.metadata)

        # Reading schema: excludes source_optional columns (will be added as nulls later)
        reading_schema_fields = [
            field for field in logical_schema.fields
            if column_operations.get(field.name, "source_required") != "source_optional"
        ]
        reading_schema = RecordSchema(fields=tuple(reading_schema_fields))

        # Output schema: excludes output_ignored columns, includes everything else
        output_schema_fields = [
            field for field in logical_schema.fields
            if column_operations.get(field.name, "source_required") != "output_ignored"
        ]
        output_schema = RecordSchema(fields=tuple(output_schema_fields))

        # Validate schema against first batch of the file if requested
        if validation:
            if file_path is None:
                raise ValueError("file_path is required when validation=True")

            backend_schema = OPS.ensure_backend_schema(reading_schema)

            # Open file handler for validation
            file_handler_result = self.reader.open(file_path, is_zip=is_zip)

            # Handle both tuple (file_handler, temp_dir) and simple file_handler returns
            if isinstance(file_handler_result, tuple):
                file_handler, temp_dir = file_handler_result
            else:
                file_handler = file_handler_result
                temp_dir = None

            try:
                batch_gen = self.reader.batch_read(file_handler, schema=backend_schema)
                first_batch = next(batch_gen)
                validated_reading_schema, optional_missing_cols = self._validate_schema_against_batch(
                    reading_schema, first_batch, file_path, column_operations
                )
                # Update reading schema with validated version (may have output_ignored removed)
                reading_schema = validated_reading_schema
            finally:
                # Close file handler after validation
                self.reader.close(file_handler, temp_dir=temp_dir)
        else:
            optional_missing_cols = set()

        # Track which source_optional columns need to be added back
        source_optional_cols = {
            field.name for field in logical_schema.fields
            if column_operations.get(field.name, "source_required") == "source_optional"
        }
        optional_missing_cols = optional_missing_cols.union(source_optional_cols)

        create_dir(output_path)

        # Get ops instance and convert output schema (excludes output_ignored) to backend schema
        backend_output_schema = OPS.ensure_backend_schema(output_schema)
        backend_metadata_schema = OPS.ensure_backend_schema(metadata_schema)

        # Unify backend schemas for writing
        writing_schema = OPS.unify_schemas([backend_output_schema, backend_metadata_schema])

        writer_cls = import_class(f"writers.{self.config.output.writer}")
        writer = writer_cls(self.writer_kwargs, output_path, schema=writing_schema)

        # Return schemas and writer:
        # - output_schema: for building batches (includes source_optional, excludes output_ignored)
        # - reading_schema: for reader (excludes source_optional)
        # - writer: configured with output_schema
        # - optional_missing_cols: source_optional columns to add as nulls
        return output_schema, reading_schema, writer, optional_missing_cols

    def process(self, d: str, search_params: Optional[Dict[str, str]] = None) -> None:
        """Processes all raw files for the specified date.

        This is the main conversion process, doing the following steps:
        1. Finds all raw files matching the desired patterns for the given date
        2. In aggregate mode: creates one writer for all files
        3. In per-file mode: creates a separate writer for each file
        4. For each file:
           - Reads data in batches
           - Adds metadata columns to each batch
           - Writes these batches to parquet
        5. Logs timing stats and row counts

        Args:
            d (str): date/delta string to process.
            search_params (Optional[Dict[str, str]]): optional search parameters for file filtering.
        """

        search_params = search_params if search_params else {}

        run_t0 = time.perf_counter()

        self.logger.info(f"Starting conversion for table: {self.table}")
        self.logger.debug(f"Raw file templates: {self.raw_file_templates}")
        for file_template in self.raw_file_templates:
            finder = FileFinder(
                file_template, extract_vars=self.extract_vars, search_params=search_params, file_type=self.file_type
            )
            self.logger.debug(str(finder))
            all_raw_files = finder.find_range(d, d)
            self.logger.debug(f"Processing {len(all_raw_files)} files for date/delta: {d}")

            # If aggregate, validate schema against first file before creating writer
            if self.aggregate:
                format_dict = {**all_raw_files[0].d_formater, "table": self.table}
                output_schema, reading_schema, writer, optional_missing_cols = self._get_schema_and_writer(
                    format_dict,
                    validation=True,
                    file_path=all_raw_files[0].full_file_path,
                    is_zip=all_raw_files[0].is_zip,
                )

            for i, raw_file in enumerate(all_raw_files, start=1):
                self.logger.info(f"[{i}/{len(all_raw_files)}] Processing file: {raw_file.full_file_path}")

                # If non-aggregate, we set up the writer and output_path once per file
                if not self.aggregate:

                    # sets up the "file_name" template. Uses member name if zip file, else file name.
                    file_name_formater = raw_file.member_name if raw_file.is_zip else raw_file.file_name
                    # remove the '.gz' extension
                    file_name_formater = file_name_formater.replace(".gz", "")
                    # remove any extra extension
                    file_name_formater = Path(file_name_formater).stem

                    format_dict = {
                        **raw_file.d_formater,
                        **raw_file.extract_vars,
                        "table": self.table,
                        "file_name": file_name_formater,
                    }
                    output_schema, reading_schema, writer, optional_missing_cols = self._get_schema_and_writer(
                        format_dict, validation=True, file_path=raw_file.full_file_path, is_zip=raw_file.is_zip
                    )

                # Convert reading schema to backend schema for reader
                backend_schema = OPS.ensure_backend_schema(reading_schema)

                # Open file handler (returns tuple for CSV with zip support, or just handler for others)
                file_handler_result = self.reader.open(raw_file.full_file_path, is_zip=raw_file.is_zip)

                # Handle both tuple (file_handler, temp_dir) and simple file_handler returns
                if isinstance(file_handler_result, tuple):
                    file_handler, temp_dir = file_handler_result
                else:
                    file_handler = file_handler_result
                    temp_dir = None

                t0 = time.perf_counter()  # timing start
                total_rows = 0
                batch_num = 0

                try:
                    for batch in self.reader.batch_read(file_handler, schema=backend_schema):

                        # Add null columns for optional missing columns
                        if optional_missing_cols:
                            # Build constants and schema_hints from output_schema
                            constants = {col: None for col in optional_missing_cols}
                            schema_hints = {
                                field.name: field.typ
                                for field in output_schema.fields
                                if field.name in optional_missing_cols
                            }
                            batch = OPS.append_constant_columns(batch, constants, schema_hints)

                        # calculate the start and end index of the batch (for _index) metadata column
                        ## Aggregate start idx is the current number of rows written across all found files
                        if self.aggregate:
                            batch_start_idx = self.num_rows
                        ## Non-Aggregate start idx is the current number of rows written from the current raw file
                        else:
                            batch_start_idx = total_rows

                        # add metadata columns to the table
                        batch = self.adjuster.add_metadata(batch, batch_start_idx, raw_file)

                        # write out the batch
                        writer.write_table(batch)

                        # increment counters
                        total_rows += batch.num_rows()
                        batch_num += 1
                finally:
                    # Close file handler and cleanup temp directory if needed
                    self.reader.close(file_handler, temp_dir=temp_dir)

                if not self.aggregate:
                    self.logger.info(f"Wrote {total_rows} row(s) in {batch_num} batch(es) to {writer.output_path}")
                    writer.close()
                elapsed = time.perf_counter() - t0  # timing end

                self.logger.info(f"Wrote {total_rows} row(s) in {batch_num} batch(es) in {format_time(elapsed)}")

                self.num_files += 1
                self.num_batches += batch_num
                self.num_rows += total_rows
                self.sum_file_seconds += elapsed

            if self.aggregate:
                self.logger.info(
                    f"Wrote {self.num_rows} row(s) in {self.num_batches} batch(es) to {writer.output_path}"
                )
                writer.close()

        run_elapsed = time.perf_counter() - run_t0
        avg_per_batch = (self.sum_file_seconds / self.num_batches) if self.num_batches else 0.0
        avg_per_file = (self.sum_file_seconds / self.num_files) if self.num_files else 0.0
        self.logger.debug(
            f"Timing summary: files={self.num_files}, batches={self.num_batches}, rows={self.num_rows}, "
            f"total={format_time(run_elapsed)}, avg_per_batch={format_time(avg_per_batch)}, avg_per_file={format_time(avg_per_file)}"
        )