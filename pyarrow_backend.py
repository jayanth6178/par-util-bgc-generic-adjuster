from typing import Any, Mapping, Optional, Union

import numpy as np
import pyarrow
import pyarrow.compute as pc

from parquet_converter.core.types import Batch, BatchOps, ListType, LogicalType, RecordSchema, TimestampType

_LOGICAL_TO_ARROW = {
    LogicalType.STRING: pyarrow.string(),
    LogicalType.INT64: pyarrow.int64(),
    LogicalType.DOUBLE: pyarrow.float64(),
    LogicalType.DATE32: pyarrow.date32(),
    LogicalType.DATE64: pyarrow.date64(),
    LogicalType.BOOL: pyarrow.bool_(),
}


def _logical_type_to_arrow(typ: Union[LogicalType, ListType, TimestampType]) -> pyarrow.DataType:
    """Convert a LogicalType, ListType, or TimestampType to a pyarrow DataType.

    This method converts our abstract, non-backend specific logical types (LogicalType, ListType,
    TimestampType) into PyArrow-specific data types for schema creation and casting.

    Args:
        typ (Union[LogicalType, ListType, TimestampType]): the logical type to convert.
            Can be a simple scalar type (LogicalType), a list type with element types
            (ListType), or a timestamp with optional timezone (TimestampType).

    Returns:
        pyarrow.DataType: the corresponding PyArrow data type.
    """

    if isinstance(typ, TimestampType):
        # Handle timezone-aware or timezone-naive timestamps
        return pyarrow.timestamp(typ.unit, tz=typ.tz)
    elif isinstance(typ, ListType):
        element_type = _logical_type_to_arrow(typ.element_type)
        return pyarrow.list_(pyarrow.field("item", element_type, nullable=typ.element_nullable))
    else:
        return _LOGICAL_TO_ARROW[typ]


class PyArrowBatch(Batch):
    """PyArrow-specific implementation of the Batch.

    Takes in a Batch object, and wraps it into a PyArrow Table. This class contains methods
    for returning number of rows, the table itself, and a specific column of the table.

    Attributes:
        _t (pyarrow.Table): the underlying PyArrow table containing the batch data.
    """

    def __init__(self, table: pyarrow.Table):
        """Initializes a PyArrow batch wrapper.

        Args:
            table (pyarrow.Table): the PyArrow table to wrap.
        """

        self._t = table

    def num_rows(self) -> int:
        """Gets the number of rows in the batch.

        Returns:
            int: row count.
        """

        return self._t.num_rows

    def unwrap(self) -> pyarrow.Table:
        """Gets the underlying PyArrow table.

        Returns:
            pyarrow.Table: the wrapped PyArrow table.
        """

        return self._t

    def __getitem__(self, column_name: str) -> pyarrow.Array:
        """Gets a specified column.

        Args:
            column_name (str): name of the column to retrieve.

        Returns:
            pyarrow.Array: the column's data as a PyArrow array.

        """

        return self._t.column(column_name)


class PyArrowOps(BatchOps):
    """PyArrow implementation of all batch operations.

    This class provides the PyArrow-specific implementations for all operations
    needed by the conversion pipeline. These operations include appending columns,
    casting types, applying compute functions, and schema management. All methods
    accept backend-independent Batch objects and logical types, converting them
    to/from PyArrow-specific types as needed.
    """

    def append_constant_columns(
        self,
        batch: Batch,
        constants: Mapping[str, Any],
        schema_hints: Optional[Mapping[str, Union[LogicalType, ListType, TimestampType]]] = None,
    ) -> Batch:
        """Appends constant-value columns to a batch.

        Creates new columns where every row has the same value. Useful for adding
        metadata columns like source file names, index, etc.

        Args:
            batch (Batch): the batch to add columns to.
            constants (Mapping[str, Any]): dictionary mapping column names to their
                constant values. Each value will be repeated for all rows.
            schema_hints (Optional[Mapping[str, Union[LogicalType, ListType, TimestampType]]]):
                optional type hints for proper casting of the constant values. If provided,
                values will be cast to the specified logical types.

        Returns:
            Batch: the batch with new constant columns appended.
        """

        t: pyarrow.Table = batch.unwrap()
        n = t.num_rows
        for name, val in constants.items():
            arr = pyarrow.repeat(val, n)
            if schema_hints and name in schema_hints:
                arr = arr.cast(_logical_type_to_arrow(schema_hints[name]))
            t = t.append_column(name, arr)
        return PyArrowBatch(t)

    def ensure_backend_schema(self, logical: RecordSchema) -> pyarrow.Schema:
        """Converts a logical schema to a PyArrow schema.

        Translates the backend-indpendent RecordSchema into a PyArrow Schema that can
        be used for reading, writing, and validating data.

        Args:
            logical (RecordSchema): the logical schema with field definitions.

        Returns:
            pyarrow.Schema: the equivalent, PyArrow schema.
        """

        fields = [pyarrow.field(f.name, _logical_type_to_arrow(f.typ), nullable=f.nullable) for f in logical.fields]
        return pyarrow.schema(fields)

    def unify_schemas(self, schemas: list[pyarrow.Schema]) -> pyarrow.Schema:
        """Combines multiple PyArrow schemas into one unified schema.

        Merges schemas by combining their fields. This is used to create a single schema
        that includes both data columns (from manifest) and metadata columns.

        Args:
            schemas (list[pyarrow.Schema]): list of PyArrow schemas to unify.

        Returns:
            pyarrow.Schema: the unified schema containing all fields.
        """

        return pyarrow.unify_schemas(schemas)

    def cast_column(
        self, batch: Batch, column_name: str, target_type: Union[LogicalType, ListType, TimestampType]
    ) -> Batch:
        """Casts a column to a different type.

        Converts an existing column to the specified logical type, replacing the
        original column in the batch. Useful for type coercion and ensuring correct
        data types before writing out to parq.

        Args:
            batch (Batch): the batch containing the column to cast.
            column_name (str): name of the column to cast.
            target_type (Union[LogicalType, ListType, TimestampType]): the logical type
                to cast the column to.

        Returns:
            Batch: the batch with the column casted to the new type.
        """

        t: pyarrow.Table = batch.unwrap()
        col = t.column(column_name).cast(_logical_type_to_arrow(target_type))
        # Replace the column in the table
        col_idx = t.schema.get_field_index(column_name)
        t = t.set_column(col_idx, column_name, col)
        return PyArrowBatch(t)

    def append_column(
        self,
        batch: Batch,
        column_name: str,
        values: Any,
        target_type: Optional[Union[LogicalType, ListType, TimestampType]] = None,
    ) -> Batch:
        """Appends a new column to the batch.

        Adds a column with the specified values to the batch. Values can be a PyArrow
        array or any data that PyArrow can convert to an array.

        Args:
            batch (Batch): the batch to add the column to.
            column_name (str): the name for the new column.
            values (Any): column data as a PyArrow array or compatible data structure.
            target_type (Optional[Union[LogicalType, ListType, TimestampType]]): optional
                logical type to cast the values to before appending.

        Returns:
            Batch: the batch with the new column appended.
        """

        t: pyarrow.Table = batch.unwrap()
        if target_type is not None:
            values = values.cast(_logical_type_to_arrow(target_type))
        t = t.append_column(column_name, values)
        return PyArrowBatch(t)

    def append_range_column(
        self, batch: Batch, column_name: str, start: int, target_type: Union[LogicalType, ListType, TimestampType]
    ) -> Batch:
        """Appends an index column to the batch.

        Creates a new column containing sequential integers starting from the specified
        start value.

        Args:
            batch (Batch): the batch to add the index column to.
            column_name (str): name for the new index column (typically "_index").
            start (int): starting value for the sequence.
            target_type (Union[LogicalType, ListType, TimestampType]): the logical type
                for the index column (typically LogicalType.INT64).

        Returns:
            Batch: the batch with the added index column.
        """

        t: pyarrow.Table = batch.unwrap()
        n = t.num_rows
        # Create a range array efficiently using numpy
        _numpy_array = np.arange(start, start + n)
        arr = pyarrow.array(_numpy_array, type=_logical_type_to_arrow(target_type))
        t = t.append_column(column_name, arr)
        return PyArrowBatch(t)

    def replace_column(
        self,
        batch: Batch,
        column_name: str,
        values: Any,
        target_type: Optional[Union[LogicalType, ListType, TimestampType]] = None,
    ) -> Batch:
        """Replaces an existing column in the batch with new values.

        Overwrites the data in an existing column while maintaining the column's position
        in the schema.

        Args:
            batch (Batch): the batch containing the column to replace.
            column_name (str): name of the column to replace.
            values (Any): new column data as a PyArrow array or compatible data structure.
            target_type (Optional[Union[LogicalType, ListType, TimestampType]]): optional
                logical type to cast the new values to.

        Returns:
            Batch: the batch with the replaced column.
        """

        t: pyarrow.Table = batch.unwrap()
        if target_type is not None:
            values = values.cast(_logical_type_to_arrow(target_type))
        # Find the column index and replace it
        col_idx = t.schema.get_field_index(column_name)
        t = t.set_column(col_idx, column_name, values)
        return PyArrowBatch(t)

    def apply(self, column: Any, func_name: str, *args, **kwargs) -> Any:
        """Applies a PyArrow compute function to a column.

        Executes a PyArrow compute function (from pyarrow.compute module) on the
        provided column. This can be specified in the config.

        Args:
            column (Any): the PyArrow array/column to apply the function to.
            func_name (str): name of the PyArrow compute function.
            *args: positional arguments to pass to the compute function.
            **kwargs: keyword arguments to pass to the compute function.

        Returns:
            Any: the result of applying the compute function (typically a PyArrow array).
        """

        func = getattr(pc, func_name)
        return func(column, *args, **kwargs)
