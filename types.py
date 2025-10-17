from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Protocol, Union, runtime_checkable


class LogicalType(Enum):
    """Backend-independent scalar data types.

    These types represent core data types that can be mapped to any backend
    (PyArrow, Polars, Pandas, etc.).

    Attributes:
        STRING: variable-length text data.
        INT64: 64-bit signed integers.
        DOUBLE: double-precision floating-point numbers (64-bit).
        DATE32: date stored as days since epoch (32-bit).
        DATE64: date stored as milliseconds since epoch (64-bit).
        BOOL: boolean true/false values.
    """

    STRING = "string"
    INT64 = "int64"
    DOUBLE = "double"
    DATE32 = "date32"
    DATE64 = "date64"
    BOOL = "bool"

@dataclass(frozen=True)
class TimestampType:
    """Timestamp type with optional timezone information.

    Represents timestamps at a time unit of microseconds, with optional timezone
    awareness. Timezone-aware timestamps explicitly store their timezone in the schema,
    while timezone-naive timestamps do not.

    Attributes:
        unit (str): time unit for the timestamp. Currently only "us" (microseconds)
            is supported. Future support may include "ms", "s", "ns".
        tz (Optional[str]): timezone string (e.g., "UTC", "America/New_York") or
            None for timezone-naive timestamps.
    """

    unit: str = "us"  # us (microseconds)
    tz: Optional[str] = None  # None for naive, "UTC", "America/New_York", etc.

    def __str__(self):
        """Returns string representation in manifest format.

        Returns:
            str: string like "timestamp[us, tz=UTC]" or "timestamp[us]".
        """

        if self.tz:
            return f"timestamp[{self.unit}, tz={self.tz}]"
        return f"timestamp[{self.unit}]"


@dataclass(frozen=True)
class ListType:
    """List/array type with element type specification.

    Represents lists where all elements have the same type. Supports nested lists and
    allows specifying whether list elements can be null.

    Attributes:
        element_type (Union[LogicalType, ListType, TimestampType]): the type of
            elements in the list.
        element_nullable (bool): whether individual elements in the list can be null.
            Defaults to True.
    """

    element_type: Union["LogicalType", "ListType", "TimestampType"]
    element_nullable: bool = True

    def __str__(self):
        """Returns string representation in manifest format.

        Returns:
            str: string like "list<string>" or "list<int64 not null>".
        """

        nullable_suffix = "" if self.element_nullable else " not null"
        if isinstance(self.element_type, ListType):
            return f"list<{str(self.element_type)}{nullable_suffix}>"
        else:
            return f"list<{self.element_type.value}{nullable_suffix}>"


@dataclass(frozen=True)
class Field:
    """Named, typed column definition.

    Represents a single column in a schema with its name, data type, and nullability.
    Fields are used to construct RecordSchemas.

    Attributes:
        name (str): column name.
        typ (Union[LogicalType, ListType, TimestampType]): the column's data type.
        nullable (bool): whether the column can contain null values.
    """

    name: str
    typ: Union[LogicalType, ListType, TimestampType]
    nullable: bool = True


@dataclass(frozen=True)
class RecordSchema:
    """Schema defining the structure of a dataset.

    A collection of Fields that together define all columns in a table. Schemas can
    be converted to backend-specific schemas (PyArrow, etc.) for actual data operations.

    Attributes:
        fields (tuple[Field, ...]): ordered tuple of field definitions.
    """

    fields: tuple[Field, ...]

    def names(self) -> tuple[str, ...]:
        """Gets all column names in the schema.

        Returns:
            tuple[str, ...]: tuple of column names in schema order.
        """

        return tuple(f.name for f in self.fields)


@runtime_checkable
class Batch(Protocol):
    """Protocol for backend-independent data batches.

    This class defines the minimal interface that any batch implementation must support.
    The batch is "opaque", meaning users interact through this protocol without knowing
    the underlying implementation details.
    """

    def num_rows(self) -> int:
        """Gets the number of rows in the batch.

        Returns:
            int: row count.
        """

        pass

    def unwrap(self) -> Any:
        """Gets the underlying backend-specific data structure.

        Returns the wrapped table/dataframe/array for backend-specific operations.
        The return type depends on the concrete implementation.

        Returns:
            Any: backend-specific data structure (e.g., pyarrow.Table, polars.DataFrame).
        """

        pass

    def __getitem__(self, column_name: str) -> Any:
        """Gets a column by name.

        Args:
            column_name (str): name of the column to retrieve.

        Returns:
            Any: backend-specific column/array object.
        """
        pass


@runtime_checkable
class BatchOps(Protocol):
    """Protocol for batch manipulation operations.

    Defines all the operations that we need to perform on batches. Each backend (PyArrow,
    Polars, etc.) provides its own implementation of these operations while maintaining
    the same interface.

    All operations work with logical types and return new batch instances.
    """

    def append_constant_columns(
        self,
        batch: Batch,
        constants: Mapping[str, Any],
        schema_hints: Mapping[str, Union[LogicalType, ListType, TimestampType]] | None = None,
    ) -> Batch:
        """Appends columns with constant values to a batch.

        Args:
            batch (Batch): batch to add columns to.
            constants (Mapping[str, Any]): column names and their constant values.
            schema_hints (Mapping[str, Union[LogicalType, ListType, TimestampType]] | None):
                optional type hints for casting values.

        Returns:
            Batch: new batch with added constant columns.
        """

        pass

    def ensure_backend_schema(self, logical: RecordSchema) -> Any:
        """Converts logical schema to backend-specific schema.

        Args:
            logical (RecordSchema): backend-independent schema.

        Returns:
            Any: backend-specific schema object (ex. pyarrow.Schema).
        """

        pass

    def unify_schemas(self, schemas: list[Any]) -> Any:
        """Combines multiple backend schemas into one.

        Args:
            schemas (list[Any]): list of backend-specific schemas to merge.

        Returns:
            Any: unified backend-specific schema.
        """

        pass

    def cast_column(
        self, batch: Batch, column_name: str, target_type: Union[LogicalType, ListType, TimestampType]
    ) -> Batch:
        """Casts a column to a different type.

        Args:
            batch (Batch): batch containing the column.
            column_name (str): name of column to cast.
            target_type (Union[LogicalType, ListType, TimestampType]): target type.

        Returns:
            Batch: new batch with column cast to new type.
        """

        pass

    def append_column(
        self,
        batch: Batch,
        column_name: str,
        values: Any,
        target_type: Union[LogicalType, ListType, TimestampType, None] = None,
    ) -> Batch:
        """Appends a new column to the batch.

        Args:
            batch (Batch): batch to add column to.
            column_name (str): name for the new column.
            values (Any): column values (backend-specific array).
            target_type (Union[LogicalType, ListType, TimestampType, None]): optional
                type to cast values to.

        Returns:
            Batch: new batch with added column.
        """

        pass

    def append_range_column(
        self, batch: Batch, column_name: str, start: int, target_type: Union[LogicalType, ListType, TimestampType]
    ) -> Batch:
        """Appends an index column to the batch.

        Args:
            batch (Batch): batch to add index column to.
            column_name (str): name for the index column.
            start (int): starting value for the sequence.
            target_type (Union[LogicalType, ListType, TimestampType]): type for the index.

        Returns:
            Batch: new batch with the added index column.
        """

        pass

    def replace_column(
        self,
        batch: Batch,
        column_name: str,
        values: Any,
        target_type: Union[LogicalType, ListType, TimestampType, None] = None,
    ) -> Batch:
        """Replaces an existing column with new values.

        Args:
            batch (Batch): batch containing the column.
            column_name (str): name of column to replace.
            values (Any): new column values (backend-specific array).
            target_type (Union[LogicalType, ListType, TimestampType, None]): optional
                type to cast values to.

        Returns:
            Batch: new batch with column replaced.
        """

        pass

    def apply(self, column: Any, func_name: str, *args, **kwargs) -> Any:
        """Applies a backend-specific compute function to a column.

        Args:
            column (Any): backend-specific column/array.
            func_name (str): name of the compute function.
            *args: positional arguments for the function.
            **kwargs: keyword arguments for the function.

        Returns:
            Any: result of applying the function (backend-specific).
        """

        pass
