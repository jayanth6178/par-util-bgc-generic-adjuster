import json
import re
from typing import Union

from parquet_converter.core.types import Field, ListType, LogicalType, RecordSchema, TimestampType
from parquet_converter.utils.config import MetadataConfig

DTYPE_MAPPING = {
    "date32": LogicalType.DATE32,
    "date64": LogicalType.DATE64,
    "date": LogicalType.DATE64,
    "int": LogicalType.INT64,
    "int64": LogicalType.INT64,
    "double": LogicalType.DOUBLE,
    "float": LogicalType.DOUBLE,
    "float64": LogicalType.DOUBLE,
    "real": LogicalType.DOUBLE,
    "decimal": LogicalType.DOUBLE,
    "numeric": LogicalType.DOUBLE,
    "number": LogicalType.DOUBLE,
    "string": LogicalType.STRING,
    "bool": LogicalType.BOOL,
}


class Manifest:
    """Handler for manifest files containing dataset schemas.

    Manifests are JSON files that define the expected schema for input data, enabling
    type enforcement during reading. This class provides utilities for reading manifests,
    parsing type strings, and generating metadata schemas.

    Future functionality (not yet implemented) includes automatic manifest generation
    by sampling input files and inferring schemas.

    Attributes:
        finder: FileFinder instance for locating sample files (used for generation).
        reader: Reader instance for reading sample files (used for generation).
        sample_size (int): Number of files to sample when auto-generating manifests.
            Default is 20.
    """

    def __init__(self, finder, reader, sample_size=20):
        """Initializes a Manifest handler.

        Args:
            finder: FileFinder instance for locating files to sample.
            reader: Reader instance for reading files to infer schemas.
            sample_size (int): Number of files to sample for schema inference.
                Default is 20.
        """

        self.finder = finder
        self.reader = reader
        self.sample_size = sample_size

    def manifest(self):
        """
        TODO: Implement manifest generation here.
            Manifest generation consists of:
            1) calling self.finder.find_all() and randomly sampling self.sample_size files
            2) calling self.reader.table() on each sample file and appending the table to a list
            3) determine the strictest possible type that each column in all files can be cast to
                a) Null columns should be cast to string
            4) combining all schemas into one schema, choosing the most restrictive type for each column
            5) resolve the manifest output path and write out schema to this path using json.dump()
            For now we are manually creating our manifests and just reading them
        """
        return

    @staticmethod
    def parse_dtype_string(dtype_str: str) -> Union[LogicalType, ListType, TimestampType]:
        """Parses a type string into a logical type object.

        Converts human-readable type strings from manifest files or configs into typed
        objects (LogicalType, ListType, or TimestampType). Supports simple types,
        timestamps with timezone specifications, and nested list types.

        Args:
            dtype_str (str): Type string to parse. Can include:
                - Simple types: 'string', 'int64', 'double', 'date32', 'bool'
                - Timestamps: 'timestamp', 'timestamp[us]', 'timestamp[us, tz=UTC]'
                - Lists: 'list<string>', 'list<int64 not null>', 'list<list<double>>'
                - Nullable modifiers: 'type not null' (handled by caller for field nullability)

        Returns:
            Union[LogicalType, ListType, TimestampType]: The parsed type object.
        """

        dtype_str = dtype_str.strip()

        # Check if it's a timestamp type with parameters
        if dtype_str.startswith("timestamp"):
            # Match patterns like:
            # - timestamp
            # - timestamp[us]
            # - timestamp[us, tz=UTC]
            # - timestamp[ms, tz=America/New_York]
            match = re.match(r"timestamp(?:\[([^\]]+)\])?", dtype_str)
            if match:
                params_str = match.group(1)

                if params_str is None:
                    # Just "timestamp" with no parameters - default to us, no timezone
                    return TimestampType(unit="us", tz=None)

                # Parse parameters
                unit = "us"  # default
                tz = None

                # Split by comma and parse each parameter
                params = [p.strip() for p in params_str.split(",")]
                for param in params:
                    if "=" in param:
                        # Named parameter like "tz=UTC"
                        key, value = param.split("=", 1)
                        key = key.strip()
                        value = value.strip()
                        if key == "tz":
                            tz = value
                    else:
                        # Positional parameter (unit)
                        unit = param

                return TimestampType(unit=unit, tz=tz)

        # Check if it's a list type
        if dtype_str.startswith("list<"):
            # Parse list type: list<element_type> or list<element_type not null>
            # Extract the content inside list<...>
            match = re.match(r"list<(.+?)>(.*)$", dtype_str)
            if not match:
                raise ValueError(f"Invalid list type format: {dtype_str}")

            inner_str = match.group(1).strip()
            outer_suffix = match.group(2).strip()

            # Check if element is nullable
            element_nullable = " not null" not in inner_str
            inner_str_clean = inner_str.replace(" not null", "").strip()

            # Recursively parse the element type (could be nested lists)
            element_type = Manifest.parse_dtype_string(inner_str_clean)

            return ListType(element_type=element_type, element_nullable=element_nullable)
        else:
            # Simple type - remove "not null" if present
            dtype_str_clean = dtype_str.replace(" not null", "").strip()
            try:
                return DTYPE_MAPPING[dtype_str_clean]
            except KeyError:
                raise ValueError(f"Unknown dtype: {dtype_str_clean}")

    @staticmethod
    def get_schema(manifest_file_path: str) -> RecordSchema:
        """Loads and parses a schema from a manifest JSON file.

        Reads a manifest file and converts it into a RecordSchema object with properly
        typed fields. Handles nullable modifiers and nested type specifications.

        Expected format:
        {"1": {"name": "col", "type": "string", "nullable": true, "column_operation": "source_required"}}

        Args:
            manifest_file_path (str): Path to the manifest JSON file containing a
                "schema" key with column-to-type mappings.

        Returns:
            RecordSchema: Schema object with Field definitions for all columns.
        """

        with open(manifest_file_path, "r") as f:
            manifest_dict = json.load(f)

        fields = []
        schema_dict = manifest_dict["schema"]

        # Sort by position to maintain order
        sorted_positions = sorted(schema_dict.keys(), key=int)

        for position in sorted_positions:
            col_spec = schema_dict[position]
            column = col_spec["name"]
            dtype_str = col_spec["type"]
            nullable = col_spec.get("nullable", True)

            # Parse the dtype string
            dtype = Manifest.parse_dtype_string(dtype_str)
            fields.append(Field(name=column, typ=dtype, nullable=nullable))

        return RecordSchema(fields=tuple(fields))

    @staticmethod
    def get_schema_with_operations(manifest_file_path: str) -> tuple[RecordSchema, dict[str, str]]:
        """Loads and parses a schema from a manifest JSON file with column operations.

        Reads a manifest file and converts it into a RecordSchema object with properly
        typed fields, and also returns a mapping of column names to their operations.

        Expected format:
        {"1": {"name": "col", "type": "string", "nullable": true, "column_operation": "source_required"}}

        Column operations:
        - "source_required": Column must exist in source file (default)
        - "source_optional": Column is optional; if missing, add with null values
        - "output_ignored": Column is ignored; don't include in output even if present in source

        Args:
            manifest_file_path (str): Path to the manifest JSON file containing a
                "schema" key with column-to-type mappings.

        Returns:
            tuple[RecordSchema, dict[str, str]]: Schema object and mapping of column names to operations
        """

        with open(manifest_file_path, "r") as f:
            manifest_dict = json.load(f)

        fields = []
        column_operations = {}
        schema_dict = manifest_dict["schema"]

        # Sort by position to maintain order
        sorted_positions = sorted(schema_dict.keys(), key=int)

        for position in sorted_positions:
            col_spec = schema_dict[position]
            column = col_spec["name"]
            dtype_str = col_spec["type"]
            nullable = col_spec.get("nullable", True)
            operation = col_spec.get("column_operation", "source_required")

            # Store the operation
            column_operations[column] = operation

            # Parse the dtype string
            dtype = Manifest.parse_dtype_string(dtype_str)
            fields.append(Field(name=column, typ=dtype, nullable=nullable))

        return RecordSchema(fields=tuple(fields)), column_operations

    @staticmethod
    def get_metadata_schema(metadata_config: MetadataConfig) -> RecordSchema:
        """Generates a schema for metadata columns based on configuration.

        Creates a RecordSchema defining all metadata columns that will be added during
        conversion, including both additional custom metadata and standard metadata
        columns.

        Args:
            metadata_config (MetadataConfig): Configuration specifying which metadata
                columns to include and their types.

        Returns:
            RecordSchema: Schema object with Field definitions for all configured
                metadata columns. Fields are ordered with additional metadata first,
                followed by standard metadata.
        """

        fields = []

        if metadata_config.additional_metadata:
            for column_name, field_config in metadata_config.additional_metadata.items():
                dtype_str = field_config.dtype
                dtype = Manifest.parse_dtype_string(dtype_str)
                fields.append(Field(name=column_name, typ=dtype, nullable=True))

        if metadata_config.standard_metadata:
            fields.extend(
                [
                    Field(name="_source_file", typ=LogicalType.STRING, nullable=True),
                    Field(name="_source_file_mtime", typ=TimestampType(unit="us", tz="UTC"), nullable=True),
                    Field(name="_creation_time", typ=TimestampType(unit="us", tz="UTC"), nullable=True),
                    Field(name="_knowledge_time", typ=TimestampType(unit="us", tz="UTC"), nullable=True),
                    Field(name="_knowledge_date", typ=LogicalType.DATE32, nullable=True),
                    Field(name="_index", typ=LogicalType.INT64, nullable=True),
                ]
            )

        return RecordSchema(fields=tuple(fields))
