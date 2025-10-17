from typing import Any

import pendulum

from parquet_converter.adjusters.base_adjusters import BaseMetadataAdjuster
from parquet_converter.core.ops_registry import get_ops
from parquet_converter.core.types import Batch, LogicalType, TimestampType
from parquet_converter.utils.config import MetadataConfig
from parquet_converter.utils.manifest import Manifest

ops = get_ops()


class StandardMetadataAdjuster(BaseMetadataAdjuster):
    """Our main class for computing and adding standard metadata to a batch of data.

    Inheriting from BaseMetadataAdjuster, this class actually implements the abstract
    methods outlined in the abstract class. The main methods of this class include
    `get_knowledge_time`, which computes the knowledge time for files according to
    the config, and `add_metadata`, which will compute and add both standard and
    additional metadata (if present in the config). Standard metadata columns are
    always added AFTER additional metadata columns.

    Attributes:
        metadata_config (MetadataConfig): an object of the MetadataConfig class. Allows us to get the specific
          preferences for the dataset at hand.
    """

    def __init__(self, metadata_config: MetadataConfig) -> None:
        """Initializes the standard metadata adjuster.

        Args:
            metadata_config (MetadataConfig): MetadataConfig object containing metadata column specifications
              and computation rules.
        """

        super().__init__(metadata_config)

    def get_knowledge_time(self, raw_file: Any) -> pendulum.DateTime:
        """Determines the knowledge time for a raw file.

        This method will determine how we want to compute the knowledge time for this batch.
        It can be derived from either the filename (parsing date components) or from the file's
        modification time, depending on the config. If knowledge_time is None or set to 'file_name'
        in the config, then this method will extract date/time components from the filename pattern
        (YYYY, MM, DD, hh, mm, ss, ms/us) and construct a datetime in the specified timezone. It will
        then convert to UTC timezone.

        Args:
            raw_file (Any): RawFileInfo object containing file metadata and path information.

        Returns:
            pendulum.DateTime: the knowledge time as a timezone-aware datetime in UTC.

        """

        # if we get the knowledge time from the file name...
        if self.metadata_config.knowledge_time is None or (
            self.metadata_config.knowledge_time is not None and self.metadata_config.knowledge_time.from_ == "file_name"
        ):
            tz = self.metadata_config.knowledge_time.tz if self.metadata_config.knowledge_time else "UTC"

            # gathers date/time information and formats to integers
            year = int(raw_file.d_formater["YYYY"])
            month = int(raw_file.d_formater["MM"])
            day = int(raw_file.d_formater["DD"]) if raw_file.d_formater["DD"] else 1
            hour = int(raw_file.d_formater["hh"]) if raw_file.d_formater["hh"] else 0
            minute = int(raw_file.d_formater["mm"]) if raw_file.d_formater["mm"] else 0
            second = int(raw_file.d_formater["ss"]) if raw_file.d_formater["ss"] else 0

            # gets microsecond
            if raw_file.d_formater["us"]:
                microsecond = int(raw_file.d_formater["us"])
            elif raw_file.d_formater["ms"]:
                microsecond = int(raw_file.d_formater["ms"]) * 1000
            else:
                microsecond = 0

            # gets the local time
            knowledge_time_local = pendulum.datetime(
                year=year, month=month, day=day, hour=hour, minute=minute, second=second, microsecond=microsecond, tz=tz
            )

            # returns the knowledge time in UTC
            return knowledge_time_local.in_timezone("UTC")

        # if we don't get the knowledge time from the file name, get it from the file mtime
        else:
            return raw_file.meta_data["file_mtime"]

    def adjust(self, adjustment: Any) -> None:
        """Applies any desired adjustments.

        Args:
            adjustment: the specific adjustment to be applied.
        """
        pass

    def add_metadata(self, batch: Batch, start_index: int, raw_file: Any) -> Batch:
        """Adds the metadata columns to an individual batch of data.

        This method appends metadata columns to the batch, including standard columns
        (ex: _source_file, _creation_time, _knowledge_time, _index) as well as any additional
        metadata columns outlined in the config. For StandardMetadataAdjuster, we intentionally
        put the additional metadata columns before the standard metadata columns. Note that
        additional metadata can optionally have functions applied (ex: rounding, string
        manipulation) before being cast to the desired dtype.

        Args:
            batch (Batch): the current batch of data that we are adding metadata to.
            start_index (int): the starting row index for this batch (for _index column).
            raw_file (Any): RawFileInfo object containing source file information.

        Returns:
            Batch: the batch with added metadata columns.
        """
        # for additional metadata columns
        if self.metadata_config.additional_metadata:
            for column_name, field_config in self.metadata_config.additional_metadata.items():

                # gets the source of the additional metadata column
                column_source = field_config.source
                dtype = Manifest.parse_dtype_string(field_config.dtype)

                # if the source is extract_vars...
                if column_source.startswith("extract_vars.") or column_source.startswith("metadata."):

                    # gets the value and dtype from the extract_var
                    var = column_source.split(".")[1]
                    value = (
                        raw_file.extract_vars[var]
                        if column_source.startswith("extract_vars.")
                        else raw_file.meta_data[var]
                    )

                    # Create a constants dict with single value to add as column
                    batch = ops.append_constant_columns(batch, {column_name: value})

                # if the source is a column, get the column and the dtype
                elif column_source.startswith("column."):
                    source_col_name = column_source.split(".")[1]
                    col = batch[source_col_name]

                    # Add the column to the batch
                    batch = ops.append_column(batch, column_name, col)

                else:
                    raise ValueError(f"Unknown source: {column_source}")

                # Apply function if specified (works for both extract_vars and column sources)
                if field_config.func:
                    func_args = field_config.func_args or []
                    func_kwargs = field_config.func_kwargs or {}
                    col = ops.apply(batch[column_name], field_config.func, *func_args, **func_kwargs)
                    batch = ops.replace_column(batch, column_name, col)

                batch = ops.cast_column(batch, column_name, dtype)

        if self.metadata_config.standard_metadata:

            knowledge_time = self.get_knowledge_time(raw_file)

            # metadata information (scalars only - no lists!)
            md = {
                "_source_file": raw_file.full_file_name,
                "_source_file_mtime": raw_file.meta_data["file_mtime"],
                "_creation_time": raw_file.creation_time,
                "_knowledge_time": knowledge_time,
                "_knowledge_date": knowledge_time.date(),
            }

            schema_hints = {
                "_source_file": LogicalType.STRING,
                "_source_file_mtime": TimestampType(unit="us", tz="UTC"),
                "_creation_time": TimestampType(unit="us", tz="UTC"),
                "_knowledge_time": TimestampType(unit="us", tz="UTC"),
                "_knowledge_date": LogicalType.DATE32,
            }

            # Add constant columns
            batch = ops.append_constant_columns(batch, md, schema_hints)

            # Add _index column as a range (memory efficient)
            batch = ops.append_range_column(batch, "_index", start_index, LogicalType.INT64)

        return batch
