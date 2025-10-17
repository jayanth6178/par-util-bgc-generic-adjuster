from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class ExtractVarConfig(BaseModel):
    """Configuration for extracting variables from file paths using regex.

    Defines how to extract values from filenames using regex and what data type to cast
    the extracted values to.

    Attributes:
        dtype (str): data type to cast the extracted value to ('string', 'int', 'float').
            Default is 'string'.
        regex_group (int): which regex capture group (1-indexed) contains the desired
            value from the filename pattern.
    """

    dtype: str = "string"
    regex_group: int


class MetadataKnowledgeTimeConfig(BaseModel):
    """Configuration for determining knowledge time from raw files.

    This specifies where to get knowledge time from and what timezone to use.

    Attributes:
        from_ (str): source of knowledge time. 'file_name' extracts from filename
            date patterns, 'file_mtime' uses file modification time.
        tz (str): timezone for the knowledge time. Default is 'UTC'.
    """

    from_: str = Field(alias="from")
    tz: str = "UTC"
    header_line: Optional[int] = 1
    header_pattern: Optional[str] = None        
    

class AdditionalMetadataFieldConfig(BaseModel):
    """Configuration for an additional metadata column.

    Defines how to create an additional metadata column beyond the standard ones.
    Columns can be sourced from extracted variables, file metadata, or existing
    data columns, with optional transformations applied.

    Attributes:
        source (str): Where to get the column value from. Format:
            - 'extract_vars.<var_name>': From regex-extracted filename variables
            - 'metadata.<field>': From file metadata (ex: 'metadata.file_mtime')
            - 'column.<name>': From an existing data column
        dtype (str): target data type for the column
        func (Optional[str]): optional PyArrow compute function to apply
        func_args (Optional[List[Any]]): positional arguments for the function.
        func_kwargs (Optional[Dict[str, Any]]): keyword arguments for the function.
    """

    source: str
    dtype: str
    func: Optional[str] = None
    func_args: Optional[List[Any]] = None
    func_kwargs: Optional[Dict[str, Any]] = None


class MetadataConfig(BaseModel):
    """Configuration for all metadata columns added during conversion.

    Controls which metadata columns are added to the output data, including both
    standard columns and additional columns.

    Attributes:
        standard_metadata (bool): whether to add standard metadata columns
        knowledge_time (Optional[MetadataKnowledgeTimeConfig]): configuration for
            computing knowledge time. If None, uses file modification time.
        additional_metadata (Optional[Dict[str, AdditionalMetadataFieldConfig]]):
            dictionary mapping column names to their configuration for custom
            metadata columns.
    """

    standard_metadata: bool = True
    knowledge_time: Optional[MetadataKnowledgeTimeConfig] = None
    additional_metadata: Optional[Dict[str, AdditionalMetadataFieldConfig]] = None


class InputConfig(BaseModel):
    """Configuration for input data source and reading.

    Specifies which reader to use, what type of files to read, and any reader-specific
    options for parsing and data conversion.

    Attributes:
        reader (str): fully-qualified reader class path relative to parquet_converter
            Default is 'pyarrow_readers.PyArrowCsvReader'.
        file_type (str): type of file pattern matching.
        encoding (Optional[str]): file encoding to use when reading files (e.g., 'utf-8',
            'latin-1'). If None, the reader will use its default encoding.
        kwargs (Optional[Dict[str, Any]]): additional keyword arguments passed directly
            to the reader's underlying read functions.
        options (Optional[Dict[str, Any]]): reader-specific options.
    """

    reader: str = "pyarrow_reader.PyArrowCsvReader"
    file_type: str = "date"
    encoding: Optional[str] = None
    kwargs: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None


class OutputConfig(BaseModel):
    """Configuration for output data destination and writing.

    Specifies the writer, adjuster, output paths, and metadata settings for the
    converted parquet files.

    Attributes:
        writer (str): fully-qualified writer class path (ex:
            'pyarrow_writers.PyArrowParquetWriter').
        adjuster (str): fully-qualified adjuster class path for adding metadata
            (ex: 'metadata_adjusters.StandardMetadataAdjuster').
        basedir (str): base directory for all output files. Output and manifest
            paths are resolved relative to this directory.
        file_template (str): template for output file paths using placeholders like
            {YYYY}, {MM}, {DD}, {table}, {file_name}.
        metadata (MetadataConfig): configuration for metadata columns to add.
        writer_kwargs (Optional[Dict[str, Any]]): additional keyword arguments
            passed to the writer.
    """

    writer: str = "pyarrow_writers.PyArrowParquetWriter"
    adjuster: str = "metadata_adjusters.StandardMetadataAdjuster"
    basedir: str
    file_template: str
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)
    writer_kwargs: Optional[Dict[str, Any]] = None


class ManifestConfig(BaseModel):
    """Configuration for manifest files containing schema definitions.

    Manifests are JSON files that define the expected schema (column names and types)
    for input data. This config controls where manifest files are located and
    how they are generated.

    Attributes:
        file_template (str): template for manifest file paths using placeholders
            like {table}. Resolved relative to output basedir.
        auto_detect_sample_size (Optional[int]): number of files to sample when
            auto-generating manifests (not yet implemented). Default is 20.
    """

    file_template: str
    auto_detect_sample_size: Optional[int] = 20


class TableConfig(BaseModel):
    """Configuration for a specific table/dataset to convert.

    Each table has its own input file patterns, aggregation settings, and optionally
    its own output file template. Multiple tables can be defined in a single config.

    Attributes:
        raw_files (List[str]): list of file path templates for finding raw files.
            Can use placeholders like {YYYY}, {MM}, {DD} and glob patterns (* and ?).
            Regex capture groups in parentheses can extract variables.
        aggregate (bool): If True, all matching files are written to a single output
            file. If False, each input file produces a separate output file.
        output_file_template (Optional[str]): optional table-specific output path
            template.
    """

    raw_files: List[str]
    aggregate: bool = False
    output_file_template: Optional[str] = None


class Config(BaseModel):
    """Main configuration for the Parquet Converter pipeline.

    This class brings together all settings for input reading,
    output writing, metadata handling, and table definitions. Validated using Pydantic
    for type safety and can be loaded from/saved to YAML files.

    Attributes:
        administrator (Optional[str]): Administrator class to use for orchestrating
            the conversion (currently unused, reserved for future).
        input (InputConfig): input data source configuration.
        output (OutputConfig): output data destination configuration.
        manifest (ManifestConfig): manifest file configuration.
        tables (Dict[str, TableConfig]): dictionary mapping table names to their
            configurations.
        extract_vars (Optional[Dict[str, ExtractVarConfig]]): optional dictionary
            mapping variable names to extraction configurations for pulling values
            from filenames.
    """

    administrator: Optional[str] = "base_administrators.ParquetConverterAdministrator"
    input: InputConfig
    output: OutputConfig
    manifest: ManifestConfig
    tables: Dict[str, TableConfig]
    extract_vars: Optional[Dict[str, ExtractVarConfig]] = None

    @field_validator("tables")
    @classmethod
    def validate_tables_not_empty(cls, v: Dict[str, TableConfig]) -> Dict[str, TableConfig]:
        """Ensures at least one table is defined in the configuration.

        Args:
            v (Dict[str, TableConfig]): the tables dictionary to validate.

        Returns:
            Dict[str, TableConfig]: the validated tables dictionary.
        """

        if not v:
            raise ValueError("At least one table must be defined in the configuration")
        return v

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> "Config":
        """Loads configuration from a YAML file.

        Parses the YAML file and validates all settings using Pydantic.

        Args:
            config_path (str | Path): path to the YAML configuration file.

        Returns:
            Config: validated configuration instance.
        """

        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)

        if config_dict is None:
            raise ValueError(f"Configuration file is empty: {config_path}")

        return cls(**config_dict)

    def to_yaml(self, output_path: str | Path) -> None:
        """Saves configuration to a YAML file.

        Exports the current configuration to YAML format, creating parent directories
        as needed.

        Args:
            output_path (str | Path): path where the YAML file should be saved.
        """

        path = Path(output_path)
        # Create parent directories if they don't exist
        path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to dict and handle aliases
        config_dict = self.model_dump(by_alias=True, exclude_none=True)

        with open(path, "w") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def generate_template(cls, output_path: str | Path) -> "Config":
        """Generates a template configuration file with example values.

        Creates a starter configuration file with typical default values and example
        settings.

        Args:
            output_path (str | Path): path where the template YAML should be saved.

        Returns:
            Config: the generated template configuration instance.
        """

        template = cls(
            administrator="base_administrators.ParquetConverterAdministrator",
            input=InputConfig(reader="pyarrow_readers.PyArrowCsvReader", file_type="date", kwargs=None),
            output=OutputConfig(
                writer="pyarrow_writers.PyArrowParquetWriter",
                adjuster="base_adjusters.StandardAdjuster",
                basedir="/path/to/output",
                file_template="1.0/raw_enriched/{YYYY}/{YYYYMMDD}/{YYYYMMDD}.{file_name}.parq",
                metadata=MetadataConfig(
                    standard_metadata=True,
                    knowledge_time=MetadataKnowledgeTimeConfig(**{"from": "file_name", "tz": "UTC"}),
                ),
                writer_kwargs=None,
            ),
            manifest=ManifestConfig(
                file_template="1.0/raw_enriched/manifest/{table}.manifest.json", auto_detect_sample_size=20
            ),
            tables={
                "example_table": TableConfig(
                    raw_files=["/path/to/raw/files/{YYYY}_{MM}_{DD}/*.csv"], aggregate=False, output_file_template=None
                )
            },
            extract_vars=None,
        )

        template.to_yaml(output_path)
        return template

    def get_table_config(self, table_name: str) -> TableConfig:
        """Gets configuration for a specific table by name.

        Retrieves the TableConfig for the specified table.

        Args:
            table_name (str): name of the table to retrieve configuration for.

        Returns:
            TableConfig: configuration for the requested table.
        """

        if table_name not in self.tables:
            available_tables = ", ".join(self.tables.keys())
            raise KeyError(f"Table '{table_name}' not found in configuration. " f"Available tables: {available_tables}")
        return self.tables[table_name]
