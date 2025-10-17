import re

import pendulum

from parquet_converter.adjusters.metadata_adjusters import StandardMetadataAdjuster
from parquet_converter.utils.config import MetadataConfig
from parquet_converter.utils.finder import RawFileInfo


class CsvHeaderKnowledgeTimeAdjuster(StandardMetadataAdjuster):
    """Generic metadata adjuster that extracts knowledge_time from CSV header using configurable pattern.

    This adjuster reads a specified line from each file and extracts the datetime using a regex pattern
    defined in the config. If the parsed datetime is timezone-naive, it's assumed to be in the timezone 
    specified in the config (default UTC).

    The adjuster caches timestamps to avoid re-reading the same file multiple times, which is
    especially important when processing files in aggregate mode.

    Attributes:
        metadata_config (MetadataConfig): config for metadata handling.
        _datetime_cache (dict): internal cache mapping file paths to extracted pendulum
            DateTime objects to avoid re-reading files.
    """

    def __init__(self, metadata_config: MetadataConfig) -> None:
        """Initializes the metadata adjuster with caching.

        Args:
            metadata_config (MetadataConfig): config specifying metadata columns,
                timezone handling, header line number, and extraction pattern.
        """
        super().__init__(metadata_config)
        self._datetime_cache = {}

    def get_knowledge_time(self, raw_file: RawFileInfo) -> pendulum.DateTime:
        """Extracts knowledge_time from the file's header using configured pattern.

        Reads the specified line from the file and extracts datetime using the regex pattern
        from config. Results are cached to avoid re-reading the same file for subsequent batches.

        Args:
            raw_file (RawFileInfo): Information about the raw file being processed.

        Returns:
            pendulum.DateTime: The extracted timestamp converted to UTC timezone.
            
        Raises:
            ValueError: If pattern not found in specified line or config missing required fields.
            RuntimeError: If file reading or datetime parsing fails.
        """
        file_path = raw_file.full_file_path

        if file_path in self._datetime_cache:
            return self._datetime_cache[file_path]

        # Validate config
        if not self.metadata_config.knowledge_time:
            raise ValueError("knowledge_time config is required for header extraction")
        
        if not self.metadata_config.knowledge_time.header_pattern:
            raise ValueError("header_pattern is required in knowledge_time config for csv_header extraction")

        header_line_num = self.metadata_config.knowledge_time.header_line or 1
        pattern = self.metadata_config.knowledge_time.header_pattern
        tz = self.metadata_config.knowledge_time.tz

        try:
            # Read the specified line from the file
            with open(file_path, "r") as f:
                for i, line in enumerate(f, start=1):
                    if i == header_line_num:
                        target_line = line.strip()
                        break
                else:
                    raise ValueError(f"File has fewer than {header_line_num} lines: {file_path}")

            # Extract datetime using pattern
            match = re.search(pattern, target_line)

            if match:
                dt_str = match.group(1)
                dt = pendulum.parse(dt_str, strict=False)

                # Apply timezone if naive
                if dt.timezone_name is None:
                    dt = dt.replace(tzinfo=pendulum.timezone(tz))

                result = dt.in_timezone("UTC")
                self._datetime_cache[file_path] = result
                return result
            else:
                raise ValueError(
                    f"Pattern '{pattern}' not found in line {header_line_num} of {file_path}. "
                    f"Line content: {target_line}"
                )

        except Exception as e:
            raise RuntimeError(f"Failed to extract knowledge_time from {file_path}: {e}") from e