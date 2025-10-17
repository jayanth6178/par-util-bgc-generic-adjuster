from abc import ABC, abstractmethod
from typing import Any

import pendulum

from parquet_converter.core.types import Batch
from parquet_converter.utils.config import MetadataConfig


class BaseAdjuster(ABC):
    """An abstract class responsible for any intermediate adjustments to raw file(s) prior to writing them out.

    Depending on the dataset/vendor, we may want to have different kinds of adjusters. The only required method at
    the moment is the abstract `adjust` method, which can be customized. Other kinds of methods can be added in classes
    that inherit this as well; it just depends on the data and desired output.
    """

    @abstractmethod
    def adjust(self, adjustment: Any) -> None:
        """Applies adjustments to the data.

        Args:
            adjustment (Any): the specific adjustment to be applied.
        """
        pass


class BaseMetadataAdjuster(BaseAdjuster):
    """An abstract class inheriting from BaseAdjuster, responsible for adding metadata columns.

    This is an abstract class with three main abstract methods that serve as a foundation for metadata column creation.
    This class allows us to have as many different ways we would like for creating the metadata columns.

    Attributes:
        metadata_config (MetadataConfig): an object of the MetadataConfig class. Allows us to get the specific
          preferences for the dataset at hand. For example, this will tell us
          if/how to compute standard metadata columns, any additional metadata
          we should be adding, etc.
    """

    def __init__(self, metadata_config: MetadataConfig) -> None:
        """Initializes the metadata adjuster.

        Args:
            metadata_config (MetadataConfig): MetadataConfig object containing metadata column specifications
              and computation rules.
        """
        self.metadata_config = metadata_config

    @abstractmethod
    def get_knowledge_time(self, raw_file: Any) -> pendulum.DateTime:
        """Determines the knowledge time for a raw file.

        Knowledge time represents the knowledge time cutoff from the vendor side, and it's typically derived from
        the filename, file modification time, or other sources depending on the dataset.

        Args:
            raw_file (Any): RawFileInfo object containing file metadata and path information.

        Returns:
            pendulum.DateTime: the knowledge time as a timezone-aware datetime in UTC.
        """
        pass

    @abstractmethod
    def adjust(self, adjustment: Any) -> None:
        """Applies any desired adjustments.

        Args:
            adjustment: the specific adjustment to be applied.
        """
        pass

    @abstractmethod
    def add_metadata(self, batch: Batch, start_index: int, end_index: int, raw_file: Any) -> Batch:
        """Adds the metadata columns to an individual batch of data.

        This method appends metadata columns to the batch, including standard columns
        (ex: _source_file, _creation_time, _knowledge_time, _index) as well as any additional
        metadata columns outlined in the config.

        Args:
            batch (Batch): the current batch of data that we are adding metadata to.
            start_index (int): the starting row index for this batch (for _index column).
            end_index (int): the ending row index for this batch (for _index column).
            raw_file (Any): RawFileInfo object containing source file information.

        Returns:
            Batch: the batch with added metadata columns.
        """
        pass
