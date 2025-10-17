import argparse
import atexit
import importlib
import logging
import os
import random
import resource
import time
from collections import defaultdict
from datetime import timedelta

import pyarrow as pa
import yaml

from parquet_converter.core.ops_registry import get_ops
from parquet_converter.core.types import LogicalType, TimestampType
from parquet_converter.ParqConverter import Administrator
from parquet_converter.readers.pyarrow_readers import BloombergReader, PyArrowCsvReader, PyArrowParquetReader
from parquet_converter.utils.finder import FileFinder
from parquet_converter.utils.manifest import Manifest

# Global logger
logger = None


def pyarrow_to_logical_type(pa_type):
    """Convert a PyArrow type to LogicalType or TimestampType.

    Args:
        pa_type (pa.DataType): PyArrow type object

    Returns:
        LogicalType | TimestampType: LogicalType enum value or TimestampType instance
    """
    if pa.types.is_string(pa_type) or pa.types.is_unicode(pa_type) or pa.types.is_large_string(pa_type):
        return LogicalType.STRING
    elif (
        pa.types.is_int64(pa_type)
        or pa.types.is_int32(pa_type)
        or pa.types.is_int16(pa_type)
        or pa.types.is_int8(pa_type)
    ):
        return LogicalType.INT64
    elif (
        pa.types.is_uint64(pa_type)
        or pa.types.is_uint32(pa_type)
        or pa.types.is_uint16(pa_type)
        or pa.types.is_uint8(pa_type)
    ):
        return LogicalType.INT64
    elif pa.types.is_float64(pa_type) or pa.types.is_float32(pa_type) or pa.types.is_decimal(pa_type):
        return LogicalType.DOUBLE
    elif pa.types.is_date32(pa_type):
        return LogicalType.DATE32
    elif pa.types.is_date64(pa_type):
        return LogicalType.DATE64
    elif pa.types.is_timestamp(pa_type):
        # Extract unit and timezone from PyArrow timestamp type
        unit = str(pa_type.unit)  # 's', 'ms', 'us', 'ns'
        tz = pa_type.tz  # timezone string or None
        return TimestampType(unit=unit, tz=tz)
    elif pa.types.is_boolean(pa_type):
        return LogicalType.BOOL
    elif pa.types.is_null(pa_type):
        # Null types default to STRING
        return LogicalType.STRING
    else:
        # Unknown types default to STRING
        return LogicalType.STRING


def _arguments():
    parser = argparse.ArgumentParser(description="Parquet Converter")
    parser.add_argument("-c", "--config", type=str, required=True, help="Path to the config file")
    parser.add_argument("-t", "--table", type=str, required=True, help="Name of the table to convert")
    parser.add_argument(
        "-s", type=int, required=False, default=5, help="Sample size of number of files to use for schema inference"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose logging (debug)")
    return parser.parse_args()


def get_config(config_path):
    """Loads config from file.

    Args:
        config_path (str): Path to config file

    Returns:
        dict: Loaded config dictionary
    """
    with open(config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return config


def get_raw_files(file_templates, extract_vars, file_type):
    """Finds all raw files matching the configured file templates.

    Args:
        file_templates (list[str]): List of file path templates with variables
        extract_vars (list[str]): Variables to extract from file paths
        file_type (str): Type of files to find (csv, parquet, etc.)

    Returns:
        list[FileInfo]: List of FileInfo objects for all matching files
    """

    for file_template in file_templates:
        finder = FileFinder(file_template, extract_vars=extract_vars, file_type=file_type)
        all_raw_files = finder.find_all()
    return all_raw_files


def get_random_sample(all_f, n):
    """
    Filter all_f to N random samples from the list.

    Args:
        all_f (list): List of files to sample from
        n (int | None): Number of random samples to return

    Returns:
        list: List of N random samples from all_f, or all_f if n is None or n >= len(all_f)
    """
    if n is None or n >= len(all_f):
        return all_f
    return random.sample(all_f, n)


def can_cast_to_common_type(type1, type2):
    """Determine the most restrictive common type between two LogicalTypes or TimestampTypes.

    Args:
        type1 (LogicalType | TimestampType): First LogicalType or TimestampType
        type2 (LogicalType | TimestampType): Second LogicalType or TimestampType

    Returns:
        LogicalType | TimestampType: Most restrictive common type that both can cast to
    """

    # If types are equal, return that type
    if type1 == type2:
        return type1

    # If both are TimestampType, try to find common type
    if isinstance(type1, TimestampType) and isinstance(type2, TimestampType):
        # If units and timezones match, they're the same
        if type1.unit == type2.unit and type1.tz == type2.tz:
            return type1
        # Otherwise, fall back to string for incompatible timestamps
        return LogicalType.STRING

    # If one is TimestampType and the other is LogicalType, fall back to string
    if isinstance(type1, TimestampType) or isinstance(type2, TimestampType):
        return LogicalType.STRING

    # Get ranks
    rank1 = get_type_hierarchy_rank(type1)
    rank2 = get_type_hierarchy_rank(type2)

    # If both are numeric, promote to the less restrictive numeric type
    if rank1 < 50 and rank2 < 50:  # Both are numeric types
        # Return the less restrictive type (higher rank)
        return type1 if rank1 > rank2 else type2

    # If both are date/timestamp types, promote to less restrictive
    if 50 <= rank1 < 60 and 50 <= rank2 < 60:
        return type1 if rank1 > rank2 else type2

    # If types are incompatible, fall back to string
    return LogicalType.STRING


# Type hierarchy from most restrictive to least restrictive
# We'll determine the strictest common type each column can be cast to
def get_type_hierarchy_rank(logical_type):
    """Return rank for type strictness (lower = more restrictive).

    Args:
        logical_type (LogicalType): LogicalType enum value

    Returns:
        int: Rank value (lower = more restrictive)
    """

    if logical_type == LogicalType.BOOL:
        return 10
    elif logical_type == LogicalType.INT64:
        return 20
    elif logical_type == LogicalType.DOUBLE:
        return 40
    elif logical_type == LogicalType.DATE32:
        return 50
    elif logical_type == LogicalType.DATE64:
        return 51
    elif logical_type == LogicalType.STRING:
        return 100
    else:
        return 200


def infer_pa_schemas(admin, all_f):
    """Infer schema by reading sample files and finding strictest common type per column.

    Args:
        admin (Administrator): Administrator object with reader configuration
        all_f (list[FileInfo]): List of FileInfo objects to sample

    Returns:
        dict[str, str]: Dict mapping column names to inferred type strings
    """

    schemas = []
    for file_info in all_f:
        logger.debug(f"Reading file: {file_info.full_file_path}")
        file_handler_result = admin.reader.open(file_info.full_file_path, is_zip=file_info.is_zip)

        # Handle both tuple (file_handler, temp_dir) and simple file_handler returns
        if isinstance(file_handler_result, tuple):
            file_handler, temp_dir = file_handler_result
        else:
            file_handler = file_handler_result
            temp_dir = None

        try:
            batch_gen = admin.reader.batch_read(file_handler, schema=None)
            first_batch = next(batch_gen, None)

            if first_batch is None:
                logger.warning(f"No data found in {file_info.full_file_path} (empty file or no batches)")
                continue

            # Get the PyArrow table from the batch using unwrap()
            table = first_batch.unwrap()
            schemas.append(table.schema)
            logger.debug(f"Schema from {file_info.full_file_path}: {table.schema}")
        finally:
            # Close file handler and cleanup
            admin.reader.close(file_handler, temp_dir=temp_dir)

    if not schemas:
        logger.error("No files could be read for schema inference")
        return

    # Step 2: Infer strictest type for each column
    # Collect all column names across all schemas, preserving order from first schema
    all_columns = list(schemas[0].names)  # Start with first schema's column order
    seen_columns = set(all_columns)

    # Add any additional columns found in other schemas
    for schema in schemas[1:]:
        for col_name in schema.names:
            if col_name not in seen_columns:
                all_columns.append(col_name)
                seen_columns.add(col_name)

    # Step 3: Combine all schemas, choosing most restrictive type for each column
    inferred_schema = {}

    for col_name in all_columns:
        col_types = []

        # Collect all types for this column across schemas and convert to LogicalType
        for schema in schemas:
            if col_name in schema.names:
                field = schema.field(col_name)
                logical_type = pyarrow_to_logical_type(field.type)
                col_types.append(logical_type)
            else:
                # Column doesn't exist in this file, treat as null -> STRING
                col_types.append(LogicalType.STRING)

        # Determine the strictest common type
        if not col_types:
            inferred_type = LogicalType.STRING
        else:
            inferred_type = col_types[0]
            for next_type in col_types[1:]:
                inferred_type = can_cast_to_common_type(inferred_type, next_type)

        # Store the type appropriately (LogicalType.value or TimestampType as-is)
        if isinstance(inferred_type, LogicalType):
            inferred_schema[col_name] = inferred_type.value
            logger.debug(f"Column '{col_name}': inferred type = {inferred_type.value}")
        else:
            # TimestampType instance - store string representation
            inferred_schema[col_name] = str(inferred_type)
            logger.debug(f"Column '{col_name}': inferred type = {str(inferred_type)}")

    # Step 4: Print the inferred schema
    # logger.info("Inferred schema:")
    # for col_name, col_type in inferred_schema.items():
    #     print(f"  {col_name}: {col_type}")

    return inferred_schema


def write_schema(schema_dict, admin, table):
    """
    Write the schema dictionary to a JSON file at the manifest path.

    Args:
        schema_dict (dict[str, str]): Dict mapping column names to type strings
        admin (Administrator): Administrator object containing configuration
        table (str): Table name for resolving manifest path
    """

    import json

    # Resolve manifest path
    manifest_template = os.path.join(admin.basedir, admin.manifest_template)
    manifest_path = manifest_template.format(table=table)

    logger.info(f"Writing schema to: {manifest_path}")

    # Convert schema_dict to new format with column ordering
    # schema_dict maintains insertion order (Python 3.7+)
    formatted_schema = {}
    for position, (col_name, col_type) in enumerate(schema_dict.items(), start=1):
        formatted_schema[str(position)] = {
            "name": col_name,
            "type": col_type,
            "column_operation": "source_required",
            "nullable": True
        }

    # Create the manifest structure
    manifest = {"schema": formatted_schema}

    # Ensure the directory exists
    manifest_dir = os.path.dirname(manifest_path)
    if manifest_dir:
        os.makedirs(manifest_dir, exist_ok=True)

    # Write to JSON file
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Schema successfully written to {manifest_path}")


def main():
    global logger

    args = _arguments()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)
    start_time = time.perf_counter()
    # atexit.register(log_on_exit, logger, start_time)

    admin = Administrator(logger, args.config, args.table)
    all_f = get_random_sample(get_raw_files(admin.raw_file_templates, admin.extract_vars, admin.file_type), args.s)

    for f in all_f:
        logger.debug(f"Using {f.full_file_path} sample file")

    logger.info(f"Found {len(all_f)} files, using {min(len(all_f), args.s)} for schema inference")

    if isinstance(admin.reader, (PyArrowCsvReader, PyArrowParquetReader, BloombergReader)):
        inferred_schema = infer_pa_schemas(admin, all_f)
    else:
        logger.error(f"Don't understand reader type {type(admin.reader)} - Please add handling for this case")

    if inferred_schema:
        write_schema(inferred_schema, admin, args.table)


if __name__ == "__main__":
    main()
